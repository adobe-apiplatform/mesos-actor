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

package com.adobe.api.platform.runtime.mesos

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorSystem
import akka.actor.Props
import akka.event.LoggingAdapter
import akka.http.scaladsl.model.HttpResponse
import akka.pattern.pipe
import java.net.URI
import org.apache.mesos.v1.Protos.AgentID
import org.apache.mesos.v1.Protos.ExecutorID
import org.apache.mesos.v1.Protos.FrameworkID
import org.apache.mesos.v1.Protos.Offer
import org.apache.mesos.v1.Protos.OfferID
import org.apache.mesos.v1.Protos.TaskID
import org.apache.mesos.v1.Protos.TaskInfo
import org.apache.mesos.v1.Protos.{ TaskState => MesosTaskState }
import org.apache.mesos.v1.Protos.TaskStatus
import org.apache.mesos.v1.scheduler.Protos.Call
import org.apache.mesos.v1.scheduler.Protos.Call._
import org.apache.mesos.v1.scheduler.Protos.Event
import org.apache.mesos.v1.scheduler.Protos.Event._
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.Failure
import scala.util.Success

//control messages
case class SubmitTask(task: TaskReqs)

case class DeleteTask(taskId: String)

case class Reconcile(tasks: Iterable[TaskRecoveryDetail])

case object Subscribe

case object Teardown

//events
case class SubscribeComplete()

case object TeardownComplete

case class TaskRecoveryDetail(taskId: String, agentId: String)

//data
case class TaskReqs(taskId: String, dockerImage: String, cpus: Double, mem: Int, port: Int)

//task states
sealed abstract class TaskState()
case class SubmitPending(reqs:TaskReqs, promise: Promise[Running]) extends TaskState
case class Submitted(taskInfo: TaskInfo, hostname: String, promise: Promise[Running]) extends TaskState
case class Running(taskInfo:TaskInfo, taskStatus:TaskStatus, hostname: String) extends TaskState
case class DeletePending(taskInfo:TaskInfo, promise: Promise[Deleted]) extends TaskState
case class Deleted(taskInfo:TaskInfo) extends TaskState

//TODO: mesos authentication
trait MesosClientActor
        extends Actor with ActorLogging with MesosClientConnection {
    implicit val ec: ExecutionContext = context.dispatcher
    implicit val logger: LoggingAdapter = context.system.log
    implicit val actorSystem: ActorSystem = context.system

    val id: String
    val frameworkName: String
    val master: String
    val role: String
    val taskMatcher: TaskMatcher
    val taskBuilder: TaskBuilder

    private lazy val frameworkID = FrameworkID.newBuilder().setValue(id).build();

    //TODO: handle redirect to master see https://github.com/mesosphere/mesos-rxjava/blob/d6fd040af3322552012fb3dcf61debb9886adbf3/mesos-rxjava-client/src/main/java/com/mesosphere/mesos/rx/java/MesosClient.java#L167
    val mesosUri = URI.create(s"${master}/api/v1/scheduler")
    var streamId: String = null
    private val tasks: TrieMap[String, TaskState] = TrieMap()

    //TODO: FSM for handling subscribing, subscribed, failed, etc states
    override def receive: Receive = {
        //control messages
        case Subscribe => {
            subscribe(frameworkID, frameworkName).pipeTo(sender())
        }
        case SubmitTask(task) => {
            val taskPromise = Promise[Running]()
            tasks += (task.taskId -> SubmitPending(task, taskPromise))
            taskPromise.future.pipeTo(sender())
        }
        case DeleteTask(taskId) => {
            val taskID = TaskID.newBuilder().setValue(taskId).build()
            val deletePromise = Promise[Deleted]()

            tasks.get(taskID.getValue) match {
            case Some(taskDetails) =>
                taskDetails match {
                    case Running(taskInfo, taskStatus,_) => {
                        val del = DeletePending(taskInfo, deletePromise)
                        tasks.update(taskId, del)
                        kill(taskID, taskStatus.getAgentId)
                    }
                    case _ => {
                        //TODO: delete tasks in other states
                        log.info("deleting non-running tasks not implemented")
                    }
                }

            case None =>
                deletePromise.failure(new Exception(s"no task was running with id ${taskId}"))
            }
            deletePromise.future.pipeTo(sender())
        }
        case Teardown => teardown.pipeTo(sender())
        case Reconcile(tasks) => reconcile(tasks)


        //event messages
        case event: Update => handleUpdate(event)
        case event: Offers => handleOffers(event)
        case event: Subscribed => handleSubscribed(event)
        case event: Event => handleHeartbeat(event)

        case msg => log.warning(s"unknown msg: ${msg}")
    }

    def handleUpdate(event: Update) = {
        log.info(s"received update for task ${event.getStatus.getTaskId.getValue} in state ${event.getStatus.getState}")

        val oldTaskDetails = tasks(event.getStatus.getTaskId.getValue)

        oldTaskDetails match {
            case Submitted(taskInfo,hostname,promise) => {
                event.getStatus.getState match {
                    case MesosTaskState.TASK_RUNNING =>
                        log.info(s"received TASK_RUNNING update for ${event.getStatus.getTaskId.getValue} and healthy? is ${event.getStatus.getHealthy}")
                        if (event.getStatus.getHealthy) {
                            //val agentHostname = agentHostnames.getOrElse(newTaskDetailsAgentId, s"unknown-agent-${newTaskDetailsAgentId}")
                            //require(hostname == agentHostname, s"hostname ${agentHostname} didn't match ${hostname}")
                            val running = Running(taskInfo, event.getStatus,hostname)
                            promise.success(running)
                            tasks.update(event.getStatus.getTaskId.getValue, running)
                        }
                    case MesosTaskState.TASK_STAGING | MesosTaskState.TASK_STARTING =>
                        log.info(s"task still launching task ${event.getStatus.getTaskId.getValue} (in state ${event.getStatus.getState}")
                    case _ =>
                        log.warning(s"failing task ${event.getStatus.getTaskId.getValue}  msg: ${event.getStatus.getMessage}")
                        promise.failure(new Exception(s"task in state ${event.getStatus.getState} msg: ${event.getStatus.getMessage}"))
                }
            }
            case Running(taskInfo,taskStatus,hostname) => {
                log.info(s"task ${taskStatus.getTaskId.getValue} changed from TASK_RUNNING to ${toCompactJsonString(event.getStatus)}")
                tasks.update(event.getStatus.getTaskId.getValue, Running(taskInfo, event.getStatus, hostname))
            }
            case DeletePending(taskInfo,promise) => {
                event.getStatus.getState match {
                case MesosTaskState.TASK_KILLED =>
                    promise.success(Deleted(taskInfo))
                case MesosTaskState.TASK_RUNNING | MesosTaskState.TASK_KILLING | MesosTaskState.TASK_STAGING | MesosTaskState.TASK_STARTING =>
                    log.info(s"task still killing task ${event.getStatus.getTaskId.getValue} (in state ${event.getStatus.getState}")
                case _ =>
                    promise.failure(new Exception(s"task ended in unexpected state ${event.getStatus.getState} msg: ${event.getStatus.getMessage}"))
                }
            }

            case _ =>
                log.warning(s"unexpected task status ${oldTaskDetails} for update event ${event}")
                tasks.foreach(t => log.info(s"      ${t}"))

        }

        acknowledge(event.getStatus)
    }

    def acknowledge(status: TaskStatus): Unit = {
        if (status.hasUuid) {
            val ack = Call.newBuilder()
                    .setType(Call.Type.ACKNOWLEDGE)
                    .setFrameworkId(frameworkID)
                    .setAcknowledge(Call.Acknowledge.newBuilder()
                            .setAgentId(status.getAgentId)
                            .setTaskId(status.getTaskId)
                            .setUuid(status.getUuid)
                            .build())
                    .build()
            execInternal(ack)

        }
    }

    def handleOffers(event: Offers) = {

        log.info(s"received ${event.getOffersList.size} offers: ${toCompactJsonString(event);}")

        val matchedTasks = taskMatcher.matchTasksToOffers(
            role,
            pending,
            event.getOffersList.asScala.toList,
            taskBuilder)

        log.info(s"matched ${matchedTasks.size} tasks out of ${pending.size} pending tasks")
        pending.foreach(reqs => {
            log.info(s"pending task: ${reqs.taskId}")
        })
        matchedTasks.values.foreach(taskInfos => {
            taskInfos.foreach(taskInfo => {
                log.info(s"     matched task: ${taskInfo.getTaskId.getValue}")
            })
        })

        //if no tasks matched, we have to explicitly decline all offers

        //if some tasks matched, we explicitly accept the matched offers, and others are implicitly declined
        if (matchedTasks.isEmpty) {
            val offerIds = asScalaBuffer(event.getOffersList).map(offer => offer.getId)

            val declineCall = Call.newBuilder
                    .setFrameworkId(frameworkID)
                    .setType(Call.Type.DECLINE)
                    .setDecline(Call.Decline.newBuilder
                            .addAllOfferIds(seqAsJavaList(offerIds)))
                    .build;

            execInternal(declineCall)

        } else {
            val acceptCall = MesosClient.accept(frameworkID,
                matchedTasks.keys.asJava,
                matchedTasks.values.flatten.asJava)

            execInternal(acceptCall).onComplete {
                case Success(r) =>
                    matchedTasks.values.flatten.map(task => {
                        tasks(task.getTaskId.getValue) match {
                            case SubmitPending(_,promise) =>
                                //dig the hostname out of the offer whose agent id matches the agent id in the task info
                                val hostname = event.getOffersList.asScala.find(p => p.getAgentId == task.getAgentId).get.getHostname
                                log.info(s"updating task ${task.getTaskId.getValue} to Submitted")
                                tasks.update(task.getTaskId.getValue, Submitted(task, hostname, promise))
                                log.info(s"done updating task ${task.getTaskId.getValue}")
                            case previousState => log.warning(s"submitted a task that was not in SubmitPending? ${previousState}")
                        }

                    })
                    if (!pending.isEmpty) {
                        log.warning("still have pending tasks after OFFER + ACCEPT: ")
                        pending.foreach(t => log.info(s"     -> ${t.taskId}"))
                    }
                case Failure(_) => log.info("failure")
            }
        }

    }

    def pending() = tasks.collect{ case (_, submitPending: SubmitPending) => submitPending.reqs}
    def handleHeartbeat(event: Event) = {
        //TODO: monitor heartbeat
        log.info(s"received heartbeat...")
    }

    def handleSubscribed(event: Subscribed) = {

        //TODO: persist to zk...
        //https://github.com/foursquare/scala-zookeeper-client
        log.info(s"subscribed; frameworkId is ${event.getFrameworkId}")
    }

    def teardown(): Future[TeardownComplete.type] = {
        log.info("submitting teardown message...")
        val teardownCall = Call.newBuilder
                .setFrameworkId(frameworkID)
                .setType(Call.Type.TEARDOWN)
                .build;

        //todo: wait for teardown...
        execInternal(teardownCall)
            .map(resp => {
                TeardownComplete
            })
    }

    def revive(): Unit = {
        log.info("submitting revive message...")
        val reviveCall = Call.newBuilder()
                .setFrameworkId(frameworkID)
                .setType(Call.Type.REVIVE)
                .build()
        execInternal(reviveCall)
    }

    def kill(taskID: TaskID, agentID: AgentID): Unit = {
        val killCall = Call.newBuilder()
                .setFrameworkId(frameworkID)
                .setType(Call.Type.KILL)
                .setKill(Kill.newBuilder()
                        .setTaskId(taskID)
                        .setAgentId(agentID)
                        .build())
                .build()
        execInternal(killCall)
    }

    def shutdown(executorID: ExecutorID, agentID: AgentID): Unit = {
        val shutdownCall = Call.newBuilder()
                .setFrameworkId(frameworkID)
                .setType(Call.Type.SHUTDOWN)
                .setShutdown(Call.Shutdown.newBuilder()
                        .setExecutorId(executorID)
                        .setAgentId(agentID)
                        .build()
                ).build()
        execInternal(shutdownCall)
    }

    //TODO: implement
    //def message
    //def request

    def reconcile(tasks: Iterable[TaskRecoveryDetail]): Unit = {

        val reconcile = Call.Reconcile.newBuilder()

        tasks.map(task => {
            reconcile.addTasks(Call.Reconcile.Task.newBuilder()
                    .setTaskId(TaskID.newBuilder().setValue(task.taskId))
                    .setAgentId(AgentID.newBuilder().setValue(task.agentId)))
        })

        val reconcileCall = Call.newBuilder()
                .setFrameworkId(frameworkID)
                .setType(Call.Type.RECONCILE)
                .setReconcile(reconcile)
                .build()
        execInternal(reconcileCall)
    }

    private def execInternal(call: Call): Future[HttpResponse] = {
        exec(call).flatMap(resp => {
            if (!resp.status.isSuccess()){
                //log and fail the future if response was not "successful"
                log.error(s"http request of type ${call.getType} returned non-successful status ${resp}")
                Future.failed(new Exception("not successful"))
            } else {
                Future.successful(resp)
            }
        })
    }

    def handleEvent(event: Event)(implicit ec: ExecutionContext):Unit = {
        log.info(s"receiving ${event.getType}")

        event.getType match {
        case Event.Type.OFFERS => self ! event.getOffers
        case Event.Type.HEARTBEAT => self ! event
        case Event.Type.SUBSCRIBED => self ! event.getSubscribed
        case Event.Type.UPDATE => self ! event.getUpdate
        case eventType => logger.warning(s"unhandled event ${toCompactJsonString(event)}")
            //todo: handle other event types
        }
    }
}

class MesosClient(val id: String, val frameworkName: String, val master: String, val role: String,
    val taskMatcher: TaskMatcher,
    val taskBuilder: TaskBuilder) extends MesosClientActor with MesosClientHttpConnection {

}

object MesosClient {

    def props(id: String, frameworkName: String, master: String, role: String,
        taskMatcher: TaskMatcher = DefaultTaskMatcher,
        taskBuilder: TaskBuilder = DefaultTaskBuilder): Props =
        Props(new MesosClient(id, frameworkName, master, role, taskMatcher, taskBuilder))

    //TODO: allow task persistence/reconcile

    def accept(frameworkId: FrameworkID, offerIds: java.lang.Iterable[OfferID], tasks: java.lang.Iterable[TaskInfo]): Call =
        Call.newBuilder
                .setFrameworkId(frameworkId)
                .setType(Call.Type.ACCEPT)
                .setAccept(Call.Accept.newBuilder
                        .addAllOfferIds(offerIds)
                        .addOperations(Offer.Operation.newBuilder
                                .setType(Offer.Operation.Type.LAUNCH)
                                .setLaunch(Offer.Operation.Launch.newBuilder
                                        .addAllTaskInfos(tasks)))).build
}

