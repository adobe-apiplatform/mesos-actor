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
import akka.pattern.ask
import akka.pattern.pipe
import akka.util.Timeout
import com.google.protobuf.util.JsonFormat
import java.net.URI
import org.apache.mesos.v1.Protos.AgentID
import org.apache.mesos.v1.Protos.ExecutorID
import org.apache.mesos.v1.Protos.FrameworkID
import org.apache.mesos.v1.Protos.Offer
import org.apache.mesos.v1.Protos.OfferID
import org.apache.mesos.v1.Protos.TaskID
import org.apache.mesos.v1.Protos.TaskInfo
import org.apache.mesos.v1.Protos.TaskStatus
import org.apache.mesos.v1.Protos.TaskStatus.Reason
import org.apache.mesos.v1.Protos.{TaskState => MesosTaskState}
import org.apache.mesos.v1.scheduler.Protos.Call
import org.apache.mesos.v1.scheduler.Protos.Call._
import org.apache.mesos.v1.scheduler.Protos.Event
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success

//control messages
case class SubmitTask(task: TaskDef)

case class DeleteTask(taskId: String)

case class Reconcile(tasks: Iterable[TaskRecoveryDetail])

case object Subscribe

case object Teardown

//events
case class SubscribeComplete(id: String)

case object TeardownComplete

case class TaskRecoveryDetail(taskId: String, agentId: String)

//task data
sealed trait Network
case object Bridge extends Network
case object Host extends Network
case class User(name: String) extends Network

sealed trait Operator
case object LIKE extends Operator
case object UNLIKE extends Operator

case class Constraint(attribute: String, operator: Operator, value: String)

case class TaskDef(taskId: String,
                   taskName: String,
                   dockerImage: String,
                   cpus: Double,
                   mem: Int,
                   ports: Seq[Int] = Seq.empty,
                   healthCheckPortIndex: Option[Int] = None,
                   forcePull: Boolean = false,
                   network: Network = Bridge,
                   dockerRunParameters: Map[String, Set[String]] = Map.empty,
                   commandDef: Option[CommandDef] = None,
                   constraints: Set[Constraint] = Set.empty)

case class CommandDef(environment: Map[String, String] = Map.empty, uris: Seq[CommandURIDef] = Seq.empty)

case class CommandURIDef(uri: URI, extract: Boolean = true, cache: Boolean = false, executable: Boolean = false)

//task states
sealed abstract class TaskState()
case class SubmitPending(reqs: TaskDef, promise: Promise[Running]) extends TaskState
case class Submitted(pending: SubmitPending,
                     taskInfo: TaskInfo,
                     offer: OfferID,
                     hostname: String,
                     hostports: Seq[Int] = List(),
                     promise: Promise[Running])
    extends TaskState
case class Running(taskId: String,
                   agentId: String,
                   taskStatus: TaskStatus,
                   hostname: String,
                   hostports: Seq[Int] = List())
    extends TaskState
case class DeletePending(taskId: String, promise: Promise[Deleted]) extends TaskState
case class Deleted(taskId: String, taskStatus: TaskStatus) extends TaskState

//TODO: mesos authentication
trait MesosClientActor extends Actor with ActorLogging with MesosClientConnection {
  implicit val ec: ExecutionContext = context.dispatcher
  implicit val logger: LoggingAdapter = context.system.log
  implicit val actorSystem: ActorSystem = context.system

  val id: () => String
  val frameworkName: String
  val failoverTimeoutSeconds: FiniteDuration
  val master: String
  val role: String
  val taskMatcher: TaskMatcher
  val taskBuilder: TaskBuilder
  val subscribeTimeout = Timeout(5 seconds)
  val autoSubscribe: Boolean
  val tasks: TaskStore
  var reconcilationData: mutable.Map[String, ReconcileTaskState] = mutable.Map()

  if (autoSubscribe) {
    log.info(s"auto-subscribing ${self} to mesos master at ${master}")
    self
      .ask(Subscribe)(subscribeTimeout)
      .mapTo[SubscribeComplete]
      .onComplete(complete => {
        log.info("subscribe completed successfully...")
        reconcilationData = mutable.Map() ++ tasks.reconcileData
        log.info(s"reconciliation data ${reconcilationData}")
        val recoveryData = reconcilationData.map(t => TaskRecoveryDetail(t._1, t._2.agentId))
        if (!recoveryData.isEmpty) {
          log.info(s"reconciling ${recoveryData.size} tasks")
          recoveryData.foreach(d => {
            log.info(s"           ${d.taskId} -> ${d.agentId}")
          })
          reconcile(recoveryData)
        }

      })
  }

  private val frameworkID = FrameworkID.newBuilder().setValue(id()).build();

  //TODO: handle redirect to master see https://github.com/mesosphere/mesos-rxjava/blob/d6fd040af3322552012fb3dcf61debb9886adbf3/mesos-rxjava-client/src/main/java/com/mesosphere/mesos/rx/java/MesosClient.java#L167
  val mesosUri = URI.create(s"${master}/api/v1/scheduler")
  var streamId: String = null
  var subscribed: Option[Future[SubscribeComplete]] = None
  //private val tasks = mutable.Map[String, TaskState]()

  //TODO: FSM for handling subscribing, subscribed, failed, etc states
  override def receive: Receive = {
    //control messages
    case Subscribe => {
      val notifyRecipient = sender()
      //handle multiple subscribe messages - only subscribe once, but reply to all on SubscribeComplete
      if (subscribed.isEmpty) {
        log.info("new subscription initiating")
        subscribed = Some(
          subscribe(frameworkID, frameworkName, failoverTimeoutSeconds.toSeconds).pipeTo(notifyRecipient))
      } else {
        log.info("subscription already initiated")
        subscribed.get.map(c => notifyRecipient ! c)
      }
    }
    case SubmitTask(task) => {
      val taskPromise = Promise[Running]()
      tasks.update(task.taskId, SubmitPending(task, taskPromise))
      taskPromise.future.pipeTo(sender())
    }
    case DeleteTask(taskId) => {
      val taskID = TaskID.newBuilder().setValue(taskId).build()
      val deletePromise = Promise[Deleted]()

      tasks.get(taskID.getValue) match {
        case Some(taskDetails) =>
          taskDetails match {
            case Running(taskId, _, taskStatus, _, _) => {
              val del = DeletePending(taskId, deletePromise)
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
    case Teardown         => teardown.pipeTo(sender())
    case Reconcile(tasks) => reconcile(tasks)

    //event messages
    case event: Event.Update     => handleUpdate(event)
    case event: Event.Offers     => handleOffers(event)
    case event: Event.Subscribed => handleSubscribed(event)
    case event: Event            => handleHeartbeat(event)

    case msg => log.warning(s"unknown msg: ${msg}")
  }

  def handleUpdate(event: Event.Update) = {
    val taskId = event.getStatus.getTaskId.getValue
    log.info(s"received update for task ${event.getStatus.getTaskId.getValue} in state ${event.getStatus.getState}")

    //TODO: handle case of unknown task?
    val oldTaskDetails = tasks.get(taskId)

    oldTaskDetails match {
      case Some(Submitted(_, taskInfo, _, hostname, hostports, promise)) => {
        event.getStatus.getState match {
          case MesosTaskState.TASK_RUNNING =>
            if (!taskInfo.hasHealthCheck || event.getStatus.getHealthy) {
              log.info(
                s"completed task launch for ${event.getStatus.getTaskId.getValue} has healthChek? ${taskInfo.hasHealthCheck} is healthy? ${event.getStatus.getHealthy}")
              val running = Running(taskId, taskInfo.getAgentId.getValue, event.getStatus, hostname, hostports)
              promise.success(running)
              tasks.update(event.getStatus.getTaskId.getValue, running)
            } else {
              log.info(s"waiting for task health for ${event.getStatus.getTaskId.getValue} ")
            }
          case MesosTaskState.TASK_STAGING | MesosTaskState.TASK_STARTING =>
            log.info(
              s"task still launching task ${event.getStatus.getTaskId.getValue} (in state ${event.getStatus.getState}")
          case _ =>
            log.warning(s"failing task ${event.getStatus.getTaskId.getValue}  msg: ${event.getStatus.getMessage}")
            promise.failure(
              new Exception(s"task in state ${event.getStatus.getState} msg: ${event.getStatus.getMessage}"))
        }
      }
      case Some(r: Running) => {
        log.info(
          s"task ${event.getStatus.getTaskId.getValue} changed from TASK_RUNNING to ${toCompactJsonString(event.getStatus)}")
        //TODO: handle TASK_LOST, etc here
        tasks.update(event.getStatus.getTaskId.getValue, r)
      }
      case Some(DeletePending(taskInfo, promise)) => {
        event.getStatus.getState match {
          case MesosTaskState.TASK_KILLED =>
            promise.success(Deleted(taskId, event.getStatus))
            tasks.remove(taskId)
          case MesosTaskState.TASK_RUNNING | MesosTaskState.TASK_KILLING | MesosTaskState.TASK_STAGING |
              MesosTaskState.TASK_STARTING =>
            log.info(
              s"task still killing task ${event.getStatus.getTaskId.getValue} (in state ${event.getStatus.getState}")
          case _ =>
            promise.failure(
              new Exception(
                s"task ended in unexpected state ${event.getStatus.getState} msg: ${event.getStatus.getMessage}"))
        }
      }
      case None if event.getStatus.getReason == Reason.REASON_RECONCILIATION => {
        event.getStatus.getState match {
          case MesosTaskState.TASK_RUNNING =>
            log.info(s"recovered task id ${taskId} in state ${event.getStatus.getState}")
            reconcilationData.remove(taskId) match {
              case Some(taskReconcileData) =>
                tasks.update(
                  taskId,
                  Running(
                    taskId,
                    taskReconcileData.agentId,
                    event.getStatus,
                    taskReconcileData.hostname,
                    taskReconcileData.hostports))
              case None =>
                log.error(s"no reconciliation data found for task ${taskId}")
            }
          case _ => log.info(s"mesos-actor does not currently reconcile tasks in state ${event.getStatus.getState}")
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
      val ack = Call
        .newBuilder()
        .setType(Call.Type.ACKNOWLEDGE)
        .setFrameworkId(frameworkID)
        .setAcknowledge(
          Call.Acknowledge
            .newBuilder()
            .setAgentId(status.getAgentId)
            .setTaskId(status.getTaskId)
            .setUuid(status.getUuid)
            .build())
        .build()
      execInternal(ack)

    }
  }

  def handleOffers(event: Event.Offers) = {

    log.info(s"received ${event.getOffersList.size} offers: ${toCompactJsonString(event);}")

    val matchedTasks = taskMatcher.matchTasksToOffers(role, pending, event.getOffersList.asScala.toList, taskBuilder)

    log.info(s"matched ${matchedTasks.size} tasks out of ${pending.size} pending tasks")
    pending.foreach(reqs => {
      log.info(s"pending task: ${reqs.taskId}")
    })
    matchedTasks.values.foreach(taskInfos => {
      taskInfos.foreach(taskInfo => {
        log.info(s"     matched task: ${taskInfo._1.getTaskId.getValue}")
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

      val taskInfos: java.lang.Iterable[TaskInfo] = matchedTasks.map(_._2.map(_._1)).flatten.asJava
      val acceptCall = MesosClient.accept(frameworkID, matchedTasks.keys.asJava, taskInfos)

      execInternal(acceptCall).onComplete {
        case Success(r) =>
          matchedTasks.foreach(entry => {
            entry._2.foreach(task => {
              tasks(task._1.getTaskId.getValue) match {
                case s @ SubmitPending(reqs, promise) =>
                  //dig the hostname out of the offer whose agent id matches the agent id in the task info
                  val hostname =
                    event.getOffersList.asScala.find(p => p.getAgentId == task._1.getAgentId).get.getHostname
                  log.info(s"updating task ${reqs.taskId} to Submitted")
                  tasks.update(reqs.taskId, Submitted(s, task._1, entry._1, hostname, task._2, promise))
                  log.info(s"done updating task ${reqs.taskId}")
                case previousState => log.warning(s"submitted a task that was not in SubmitPending? ${previousState}")
              }
            })

          })
          if (!pending.isEmpty) {
            log.warning("still have pending tasks after OFFER + ACCEPT: ")
            pending.foreach(t => log.info(s"     -> ${t.taskId}"))
          }
        case Failure(_) => log.info("failure")
      }
    }

  }

  def pending() = tasks.collect { case (_, submitPending: SubmitPending) => submitPending.reqs }
  def handleHeartbeat(event: Event) = {
    //TODO: monitor heartbeat
    log.info(s"received heartbeat...")
  }

  def handleSubscribed(event: Event.Subscribed) = {

    //TODO: persist to zk...
    //https://github.com/foursquare/scala-zookeeper-client
    log.info(s"subscribed; frameworkId is ${event.getFrameworkId} streamId is ${streamId}")
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
    val reviveCall = Call
      .newBuilder()
      .setFrameworkId(frameworkID)
      .setType(Call.Type.REVIVE)
      .build()
    execInternal(reviveCall)
  }

  def kill(taskID: TaskID, agentID: AgentID): Unit = {
    val killCall = Call
      .newBuilder()
      .setFrameworkId(frameworkID)
      .setType(Call.Type.KILL)
      .setKill(
        Kill
          .newBuilder()
          .setTaskId(taskID)
          .setAgentId(agentID)
          .build())
      .build()
    execInternal(killCall)
  }

  def shutdown(executorID: ExecutorID, agentID: AgentID): Unit = {
    val shutdownCall = Call
      .newBuilder()
      .setFrameworkId(frameworkID)
      .setType(Call.Type.SHUTDOWN)
      .setShutdown(
        Call.Shutdown
          .newBuilder()
          .setExecutorId(executorID)
          .setAgentId(agentID)
          .build())
      .build()
    execInternal(shutdownCall)
  }

  //TODO: implement
  //def message
  //def request

  def reconcile(tasks: Iterable[TaskRecoveryDetail]): Unit = {

    val reconcile = Call.Reconcile.newBuilder()

    tasks.map(task => {
      reconcile.addTasks(
        Call.Reconcile.Task
          .newBuilder()
          .setTaskId(TaskID.newBuilder().setValue(task.taskId))
          .setAgentId(AgentID.newBuilder().setValue(task.agentId)))
    })

    val reconcileCall = Call
      .newBuilder()
      .setFrameworkId(frameworkID)
      .setType(Call.Type.RECONCILE)
      .setReconcile(reconcile)
      .build()
    execInternal(reconcileCall)
  }

  private def execInternal(call: Call): Future[HttpResponse] = {
    exec(call).flatMap(resp => {
      if (!resp.status.isSuccess()) {
        //log and fail the future if response was not "successful"
        log.error(s"http request of type ${call.getType} returned non-successful status ${resp}")
        Future.failed(new Exception("not successful"))
      } else {
        Future.successful(resp)
      }
    })
  }

  def handleEvent(event: Event)(implicit ec: ExecutionContext): Unit = {
    log.info(s"receiving ${event.getType}")

    event.getType match {
      case Event.Type.OFFERS     => self ! event.getOffers
      case Event.Type.HEARTBEAT  => self ! event
      case Event.Type.SUBSCRIBED => self ! event.getSubscribed
      case Event.Type.UPDATE     => self ! event.getUpdate
      case Event.Type.RESCIND =>
        val offerId = event.getRescind.getOfferId
        //for tasks in submitted state with this offer, resubmit them
        tasks.foreach(ts => {
          ts._2 match {
            case s: Submitted if s.offer == offerId =>
              logger.warning(s"offer ${offerId.getValue} rescinded before accept was complete for task ${ts._1} ")
              tasks.update(ts._1, s.pending)
            case _ => //nothing
          }
        })
      case Event.Type.FAILURE => logger.warning(s"received failure message ${event.getFailure}")
      case Event.Type.ERROR   => logger.error(s"received error ${event.getError}")
      case eventType          => logger.warning(s"unhandled event ${toCompactJsonString(event)}")

    }
  }
  def toCompactJsonString(message: com.google.protobuf.Message) =
    JsonFormat.printer.omittingInsignificantWhitespace.print(message)

}

class MesosClient(val id: () => String,
                  val frameworkName: String,
                  val master: String,
                  val role: String,
                  val failoverTimeoutSeconds: FiniteDuration,
                  val taskMatcher: TaskMatcher,
                  val taskBuilder: TaskBuilder,
                  val autoSubscribe: Boolean,
                  val tasks: TaskStore)
    extends MesosClientActor
    with MesosClientHttpConnection {}

object MesosClient {

  def props(id: () => String,
            frameworkName: String,
            master: String,
            role: String,
            failoverTimeoutSeconds: FiniteDuration,
            taskMatcher: TaskMatcher = new DefaultTaskMatcher,
            taskBuilder: TaskBuilder = new DefaultTaskBuilder,
            autoSubscribe: Boolean = false,
            taskStore: TaskStore): Props =
    Props(
      new MesosClient(
        id,
        frameworkName,
        master,
        role,
        failoverTimeoutSeconds,
        taskMatcher,
        taskBuilder,
        autoSubscribe,
        taskStore))

  //TODO: allow task persistence/reconcile

  def accept(frameworkId: FrameworkID,
             offerIds: java.lang.Iterable[OfferID],
             tasks: java.lang.Iterable[TaskInfo]): Call =
    Call.newBuilder
      .setFrameworkId(frameworkId)
      .setType(Call.Type.ACCEPT)
      .setAccept(
        Call.Accept.newBuilder
          .addAllOfferIds(offerIds)
          .addOperations(
            Offer.Operation.newBuilder
              .setType(Offer.Operation.Type.LAUNCH)
              .setLaunch(Offer.Operation.Launch.newBuilder
                .addAllTaskInfos(tasks))))
      .build

}
