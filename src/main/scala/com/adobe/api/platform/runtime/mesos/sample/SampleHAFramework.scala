/*
 * Copyright 2017 Adobe Systems Incorporated. All rights reserved.
 *
 * This file is licensed to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR REPRESENTATIONS OF ANY KIND,
 * either express or implied.  See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.adobe.api.platform.runtime.mesos.sample

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.PoisonPill
import akka.actor.Props
import akka.actor.Stash
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.cluster.ddata.DistributedData
import akka.cluster.ddata.LWWMap
import akka.cluster.ddata.LWWMapKey
import akka.cluster.ddata.Replicator.Get
import akka.cluster.ddata.Replicator.GetSuccess
import akka.cluster.ddata.Replicator.ReadLocal
import akka.cluster.ddata.Replicator.Update
import akka.cluster.ddata.Replicator.WriteLocal
import akka.cluster.singleton.ClusterSingletonManager
import akka.cluster.singleton.ClusterSingletonManagerSettings
import akka.cluster.singleton.ClusterSingletonProxy
import akka.cluster.singleton.ClusterSingletonProxySettings
import akka.management.AkkaManagement
import akka.management.cluster.bootstrap.ClusterBootstrap
import akka.pattern.ask
import akka.util.Timeout
import com.adobe.api.platform.runtime.mesos._
import java.time.Instant
import java.util.UUID

import scala.concurrent.Future
import scala.concurrent.duration._

object SampleHAFramework {

  var taskCount = 0
  val taskLaunchTimeout = Timeout(15 seconds)
  val taskDeleteTimeout = Timeout(10 seconds)

  def nextName() = {
    taskCount += 1
    s"sample-task-${Instant.now.getEpochSecond}-${taskCount}"
  }
  def nextId() = "sample-task-" + UUID.randomUUID()

  val system = ActorSystem("sample-ha-framework")
  implicit val ec = system.dispatcher
  implicit val log = system.log
  implicit val cluster = Cluster(system)
  val replicator: ActorRef = DistributedData(system).replicator

  var frameworkId: Option[String] = None
  def main(args: Array[String]): Unit = {

    val taskLaunchTimeout = Timeout(30 seconds)
    val taskDeleteTimeout = Timeout(10 seconds)
    // Create an actor that handles cluster domain events
    system.actorOf(Props(SimpleClusterListener), name = "clusterListener")

    //use akka management + cluster bootstrap to init the cluster
    AkkaManagement(system).start()
    ClusterBootstrap(system).start()
    //create task store
    val tasks = new DistributedDataTaskStore(system)

    //create the singleton MesosClient actor
    system.actorOf(
      ClusterSingletonManager.props(
        MesosClient.props(
          () => frameworkId.getOrElse("sample-" + UUID.randomUUID()),
          "sample-framework",
          "http://192.168.99.100:5050",
          "sample-role",
          30.seconds,
          autoSubscribe = true,
          taskStore = tasks),
        terminationMessage = PoisonPill,
        settings = ClusterSingletonManagerSettings(system)),
      name = "mesosClientMaster")
    val mesosClientActor = system.actorOf(
      ClusterSingletonProxy
        .props(singletonManagerPath = "/user/mesosClientMaster", settings = ClusterSingletonProxySettings(system)),
      name = "mesosClientProxy")

    log.info("waiting for subscription to complete")
    mesosClientActor
      .ask(Subscribe)(taskLaunchTimeout)
      .mapTo[SubscribeComplete]
      .map(c => {
        log.info("subscribe completed successfully...")
        frameworkId = Some(c.id)
        var taskCount = 0
        def nextName() = {
          taskCount += 1
          s"cluster-task-${cluster.selfUniqueAddress}-${taskCount}"
        }

        def nextId() = "sample-task-" + UUID.randomUUID()

        val launches = Future.sequence((1 to 2).map(_ => {
          val taskId = nextId()
          log.info(s"launching task id ${taskId}")
          val task =
            TaskDef(
              taskId,
              nextName(),
              "trinitronx/python-simplehttpserver",
              0.1,
              24,
              List(8080, 8081),
              Some(HealthCheckConfig(0)),
              commandDef = Some(CommandDef()))
          val launched: Future[TaskState] = mesosClientActor.ask(SubmitTask(task))(taskLaunchTimeout).mapTo[TaskState]
          launched map {
            case taskDetails: Running => {
              val taskHost = taskDetails.hostname
              val taskPorts = taskDetails.hostports
              log.info(
                s"launched task id ${taskDetails.taskId} with state ${taskDetails.taskStatus.getState} on agent ${taskHost} listening on ports ${taskPorts}")

            }
            case s => log.error(s"failed to launch task; state is ${s}")
          } recover {
            case t => log.error(s"task launch failed ${t.getMessage}", t)
          }
          launched
        }))

        //after we launch some tasks, wait around for 60s - during this time, you can cause a failover (kill the framework leader), and another node will become leader
        //whichever node is the leader will enumerate the running tasks and kill them, assuming reconciliation went fine.
        launches.onComplete(f => {
          //schedule delete in 30 seconds, for ALL tasks
          system.scheduler.scheduleOnce(60.seconds) {
            if (tasks.isEmpty) {
              log.info("this cluster is not the framework - tasks will be killed by the framework node")
            } else {
              tasks.foreach(t => {
                log.info(s"removing previously created task ${t._1}")
                mesosClientActor
                  .ask(DeleteTask(t._1))(taskDeleteTimeout)
                  .mapTo[Deleted]
                  .map(deleted => {
                    log.info(s"task killed ended with state ${deleted.taskStatus.getState}")
                  })

              })
            }

          }
        })

      })

  }

  val key = "FWID"
  implicit val askTimeout = Timeout(30.seconds)

  def dataKey(entryKey: String): LWWMapKey[String, Any] =
    LWWMapKey("cache-" + math.abs(entryKey.hashCode) % 100)

  def getFWID() = {

    log.info("getting frameworkid...")
    //check for NotFound
    val cacheResult = replicator.ask(Get(dataKey(key), ReadLocal, Some(Request(key))))
    cacheResult.map {
      case s: GetSuccess[_] => {
        s.dataValue match {
          case data: LWWMap[_, _] =>
            data.asInstanceOf[LWWMap[String, String]].get(key) match {
              case Some(fwid) =>
                log.info(s"returning cached fwid ${fwid}")
                fwid
              case None => {
                val fwid = "sample-" + UUID.randomUUID()
                log.info(s"found but none; returning new fwid ${fwid}")
                fwid
              }
            }
          case _ => {
            val fwid = "sample-" + UUID.randomUUID()
            log.info(s"found wrong data; returning new fwid ${fwid}")
            fwid
          }
        }
      }
      case _ => {
        val fwid = "sample-" + UUID.randomUUID()
        log.info(s"none found; returning new fwid ${fwid}")
        log.info("updating cache with new FWID")
        replicator ! Update(dataKey(key), LWWMap(), WriteLocal)(_ + (key -> fwid))
        fwid
      }
    }
  }
}

private final case class Request(key: String)

object SimpleClusterListener extends Actor with ActorLogging with Stash {
  implicit val cluster = Cluster(context.system)

  // subscribe to cluster changes, re-subscribe when restart
  override def preStart(): Unit = {
    cluster.subscribe(self, classOf[MemberEvent], classOf[UnreachableMember])
  }
  override def postStop(): Unit = cluster.unsubscribe(self)

  def receive = {
    case UnreachableMember(member) =>
      log.info("Explicitly downing unreachable node: {}", member)
      Cluster.get(context.system).down(member.address)
    case _ =>
    // ignore

  }

}
