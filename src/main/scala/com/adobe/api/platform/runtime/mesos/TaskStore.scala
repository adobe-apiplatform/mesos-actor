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
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.cluster.Cluster
import akka.cluster.ddata.DistributedData
import akka.cluster.ddata.LWWRegister
import akka.cluster.ddata.LWWRegisterKey
import akka.cluster.ddata.Replicator
import akka.cluster.ddata.Replicator.Get
import akka.cluster.ddata.Replicator.GetSuccess
import akka.cluster.ddata.Replicator.ReadLocal
import akka.cluster.ddata.Replicator.WriteLocal
import akka.pattern.ask
import akka.util.Timeout
import com.adobe.api.platform.runtime.mesos.ReplicatedCache.GetReconcileData
import com.adobe.api.platform.runtime.mesos.ReplicatedCache.GetReconcilePendingData
import com.adobe.api.platform.runtime.mesos.ReplicatedCache.Register
import com.adobe.api.platform.runtime.mesos.ReplicatedCache.RegisterPending
import com.adobe.api.platform.runtime.mesos.ReplicatedCache.UnRegister
import com.adobe.api.platform.runtime.mesos.ReplicatedCache.UnRegisterPending
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration._

/**
 * TaskStore provides an API for storing task status as well as generating data required to reconcile tasks
 */
trait TaskStore {
  protected val tasks = mutable.Map[String, TaskState]()
  def reconcileData: Future[Map[String, ReconcileTaskState]]
  def reconcilePendingData: Future[Map[String, ReconcilePending]]
  def update(taskId: String, taskState: TaskState)
  def remove(taskId: String)
  def get(taskId: String): Option[TaskState] = tasks.get(taskId)
  def foreach[U](f: ((String, TaskState)) => U): Unit = tasks.foreach(f)
  def collect[B, That](pf: PartialFunction[(String, TaskState), B]) = tasks.collect(pf)
  def isEmpty = tasks.isEmpty
}

/**
 * A volatile memory TaskStore impl; does not help in failover scenarios
 */
class LocalTaskStore extends TaskStore {

  def reconcileData: Future[Map[String, ReconcileTaskState]] =
    Future.successful(Map.empty)

  def reconcilePendingData: Future[Map[String, ReconcilePending]] =
    Future.successful(Map.empty)

  override def update(taskId: String, taskState: TaskState): Unit = tasks.update(taskId, taskState)

  override def remove(taskId: String): Unit = tasks.remove(taskId)
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
    (cacheActor ? GetReconcileData)(Timeout(60.seconds)).mapTo[Map[String, ReconcileTaskState]]

  def reconcilePendingData =
    (cacheActor ? GetReconcilePendingData)(Timeout(60.seconds)).mapTo[Map[String, ReconcilePending]]
  override def update(taskId: String, taskState: TaskState) = {
    //capture the current taskState for local usage
    tasks.update(taskId, taskState)
    //if its Running, replicate the cache state for reconciliation needs
    taskState match {
      case SubmitPending(taskDef, _, _) =>
        cacheActor ! RegisterPending(taskId, ReconcilePending(taskDef))
      case Submitted(_, taskInfo, offer, host, hostPorts, _) =>
        cacheActor ! UnRegisterPending(taskId) //no longer pending, so don't track with pending data
        cacheActor ! Register(taskId, ReconcileTaskState(taskInfo.getAgentId.getValue, host, hostPorts))
      case Running(_, agentId, _, hostname, hostports) =>
        cacheActor ! Register(taskId, ReconcileTaskState(agentId, hostname, hostports))
      case _ => //nothing
    }
  }

  override def remove(id: String) = {
    //capture the current taskState for local usage
    tasks.remove(id)
    //always replicate the cache state for removal
    cacheActor ! UnRegisterPending(id)
    cacheActor ! UnRegister(id)
  }
}

/**
 * A akka-friendly (ReplicatedData) snapshot of task details required for reconciliation
 * @param agentId
 * @param hostname
 * @param hostports
 */
case class ReconcileTaskState(agentId: String, hostname: String, hostports: Seq[Int]) extends Serializable

/**
 * Replicated data type for unlaunched tasks
 * @param taskDef
 */
case class ReconcilePending(taskDef: TaskDef) extends Serializable

private object ReplicatedCache {
  final case object GetReconcileData
  final case object GetReconcilePendingData
  final case class Register(taskId: String, taskState: ReconcileTaskState)
  final case class RegisterPending(taskId: String, taskDef: ReconcilePending)
  final case class UnRegister(taskId: String)
  final case class UnRegisterPending(taskId: String)
  def props()(implicit cluster: Cluster) = Props(new ReplicatedCache())
}

private class ReplicatedCache()(implicit val cluster: Cluster) extends Actor with ActorLogging {
  private val replicator = DistributedData(context.system).replicator

  private val TasksKey = LWWRegisterKey[Map[String, ReconcileTaskState]]("mesosActorTasks")
  private val PendingTasksKey = LWWRegisterKey[Map[String, ReconcilePending]]("mesosActorPendingTasks")

  private var cachedData = Map.empty[String, ReconcileTaskState]
  private var cachedPendingData = Map.empty[String, ReconcilePending]

  def receive = {
    case GetReconcileData =>
      replicator ! Get(TasksKey, ReadLocal, Some(sender()))
    case GetReconcilePendingData =>
      replicator ! Get(PendingTasksKey, ReadLocal, Some(sender()))
    case g @ GetSuccess(TasksKey, Some(replyTo: ActorRef)) =>
      replyTo ! g.get(TasksKey).value
    case g @ GetSuccess(PendingTasksKey, Some(replyTo: ActorRef)) =>
      replyTo ! g.get(PendingTasksKey).value
    case Register(taskId, taskState) =>
      cachedData = cachedData + (taskId -> taskState)
      replicator ! Replicator.Update(TasksKey, LWWRegister[Map[String, ReconcileTaskState]](Map.empty), WriteLocal)(
        reg => reg.withValue(cachedData))
    case RegisterPending(taskId, taskDef) =>
      cachedPendingData = cachedPendingData + (taskId -> taskDef)
      replicator ! Replicator.Update(
        PendingTasksKey,
        LWWRegister[Map[String, ReconcilePending]](Map.empty),
        WriteLocal)(reg => reg.withValue(cachedPendingData))
    case UnRegister(taskId) =>
      cachedData = cachedData - taskId
      replicator ! Replicator.Update(TasksKey, LWWRegister[Map[String, ReconcileTaskState]](Map.empty), WriteLocal)(
        reg => reg.withValue(cachedData))
      //in case a task is deleted before it was running, also cleanup the pending taskDef
      if (cachedPendingData.keySet.contains(taskId)) {
        cachedPendingData = cachedPendingData - taskId
        replicator ! Replicator.Update(
          PendingTasksKey,
          LWWRegister[Map[String, ReconcilePending]](Map.empty),
          WriteLocal)(reg => reg.withValue(cachedPendingData))
      }
    case UnRegisterPending(taskId) =>
      cachedPendingData = cachedPendingData - taskId
      replicator ! Replicator.Update(
        PendingTasksKey,
        LWWRegister[Map[String, ReconcilePending]](Map.empty),
        WriteLocal)(reg => reg.withValue(cachedPendingData))
  }

}
