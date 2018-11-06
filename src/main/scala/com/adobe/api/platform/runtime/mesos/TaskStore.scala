/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.adobe.api.platform.runtime.mesos

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorSystem
import akka.actor.Props
import akka.cluster.Cluster
import akka.cluster.ddata.DistributedData
import akka.cluster.ddata.Key
import akka.cluster.ddata.LWWMapKey
import akka.cluster.ddata.ORMap
import akka.cluster.ddata.ORMapKey
import akka.cluster.ddata.ReplicatedData
import akka.cluster.ddata.Replicator
import akka.pattern.ask
import akka.util.Timeout
import com.adobe.api.platform.runtime.mesos.ReplicatedCache.GetReconcileData
import com.adobe.api.platform.runtime.mesos.ReplicatedCache.Register
import com.adobe.api.platform.runtime.mesos.ReplicatedCache.TaskDataKey
import com.adobe.api.platform.runtime.mesos.ReplicatedCache.UnRegister
import scala.collection.immutable
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._

/**
 * TaskStore provides an API for storing task status as well as generating data required to reconcile tasks
 */
trait TaskStore extends immutable.Map[String, TaskState] {
  def reconcileData: Map[String, ReconcileTaskState]
  def update(taskId: String, taskState: TaskState)
  def remove(taskId: String)
  protected val tasks = mutable.Map[String, TaskState]()
}

/**
 * A volatile memory TaskStore impl; does not help in failover scenarios
 */
class LocalTaskStore extends TaskStore {

  def reconcileData: Map[String, ReconcileTaskState] =
    tasks
      .collect({
        case (_, Running(taskId, agentId, _, hostname, hostports)) =>
          (taskId, ReconcileTaskState(agentId, hostname, hostports))

      })
      .toMap
  override def update(taskId: String, taskState: TaskState): Unit = tasks.update(taskId, taskState)

  override def remove(taskId: String): Unit = tasks.remove(taskId)

  override def +[B1 >: TaskState](kv: (String, B1)): Map[String, B1] = (tasks + kv).toMap

  override def get(key: String): Option[TaskState] = tasks.get(key)

  override def iterator: Iterator[(String, TaskState)] = tasks.iterator

  override def -(key: String): Map[String, TaskState] = (tasks - key).toMap
}

/**
 * A distributed task store that uses the akka cluster to replicate task data to all nodes.
 * During a failover, reconcileData can expose the replicated task data to the mesos framework to recover on a new node.
 * @param as
 * @param cluster
 */
class DistributedDataTaskStore(as: ActorSystem)(implicit cluster: Cluster) extends TaskStore {
  private val cacheActor = as.actorOf(ReplicatedCache.props())

  def reconcileData =
    Await.result((cacheActor ? GetReconcileData)(Timeout(5.seconds)).mapTo[Map[String, ReconcileTaskState]], 5.seconds)

  override def update(taskId: String, taskState: TaskState) = {
    //capture the current taskState for local usage
    tasks += (taskId -> taskState)
    //if its Running, replicate the cache state for reconciliation needs
    taskState match {
      case Running(taskInfo, agentId, _, hostname, hostports) =>
        cacheActor ! Register(taskId, ReconcileTaskState(agentId, hostname, hostports))
      case _ => //nothing
    }

  }

  override def remove(id: String) = {
    //capture the current taskState for local usage
    tasks -= (id)
    //always replicate the cache state for removal
    cacheActor ! UnRegister(id)
  }
  def taskKey(taskId: String) = LWWMapKey[String, String](taskId)

  override def +[B1 >: TaskState](kv: (String, B1)) = (tasks + kv).toMap

  override def get(key: String) = tasks.get(key)

  override def iterator = tasks.iterator

  override def -(key: String) = (tasks - key).toMap
}

/**
 * A akka-friendly (ReplicatedData) snapshot of task details required for reconciliation
 * @param agentId
 * @param hostname
 * @param hostports
 */
case class ReconcileTaskState(val agentId: String, val hostname: String, val hostports: Seq[Int])
    extends ReplicatedData
    with Serializable {
  override type T = this.type
  override def merge(that: ReconcileTaskState.this.type) =
    //no merging supported, just return this; we only have 1 writer at any time, so merging should not happen
    this
}

private object ReplicatedCache {
  final case object GetReconcileData
  final case class Register(taskId: String, taskState: ReconcileTaskState)
  final case class UnRegister(taskId: String)

  final case class TaskDataKey(taskId: String) extends Key[ORMap[String, ReconcileTaskState]](taskId)
  //the key for the reconciliation data
  private val RecoveryDataKey = ORMapKey[TaskDataKey, ReconcileTaskState]("task-recovery-data")
  def props()(implicit cluster: Cluster) = Props(new ReplicatedCache())
}

private class ReplicatedCache()(implicit val cluster: Cluster) extends Actor with ActorLogging {
  val replicator = DistributedData(context.system).replicator
  var cachedData = Map.empty[String, ReconcileTaskState]
  def taskDataKey(taskId: String): TaskDataKey = TaskDataKey(taskId)

  override def preStart(): Unit = {
    replicator ! Replicator.Subscribe(ReplicatedCache.RecoveryDataKey, self)
  }

  override def postStop(): Unit = {
    cluster.unsubscribe(self)
  }

  def receive = {
    case GetReconcileData =>
      sender() ! cachedData
    case Register(taskId, taskState) =>
      replicator ! Replicator.Update(
        ReplicatedCache.RecoveryDataKey,
        ORMap.empty[TaskDataKey, ReconcileTaskState],
        Replicator.WriteLocal)(_ + (taskDataKey(taskId), taskState))
    case UnRegister(taskId) =>
      replicator ! Replicator.Update(
        ReplicatedCache.RecoveryDataKey,
        ORMap.empty[TaskDataKey, ReconcileTaskState],
        Replicator.WriteLocal)(_ - taskDataKey(taskId))
    case c @ Replicator.Changed(ReplicatedCache.RecoveryDataKey) =>
      cachedData = c.get(ReplicatedCache.RecoveryDataKey).entries.map(e => (e._1.id -> e._2))
      log.debug(s"new cache data value ${cachedData}")

  }

}
