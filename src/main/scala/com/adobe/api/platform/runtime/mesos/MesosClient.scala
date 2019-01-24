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
import akka.actor.Cancellable
import akka.actor.Props
import akka.event.LoggingAdapter
import akka.http.scaladsl.model.HttpResponse
import akka.pattern.ask
import akka.pattern.pipe
import akka.util.Timeout
import com.google.protobuf.util.JsonFormat
import java.net.URI
import java.time.Instant
import org.apache.mesos.v1.Protos.AgentID
import org.apache.mesos.v1.Protos.ExecutorID
import org.apache.mesos.v1.Protos.Filters
import org.apache.mesos.v1.Protos.FrameworkID
import org.apache.mesos.v1.Protos.Offer
import org.apache.mesos.v1.Protos.OfferID
import org.apache.mesos.v1.Protos.TaskID
import org.apache.mesos.v1.Protos.TaskInfo
import org.apache.mesos.v1.Protos.TaskStatus
import org.apache.mesos.v1.Protos.{TaskState => MesosTaskState}
import org.apache.mesos.v1.Protos.TaskStatus.Reason
import org.apache.mesos.v1.scheduler.Protos.Call
import org.apache.mesos.v1.scheduler.Protos.Call._
import org.apache.mesos.v1.scheduler.Protos.Event
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

case object Heartbeat

//task data
sealed trait Network
case object Bridge extends Network
case object Host extends Network
case class User(name: String) extends Network

//constraints
sealed trait Operator
case object LIKE extends Operator
case object UNLIKE extends Operator

case class Constraint(attribute: String, operator: Operator, value: String)

//subscription state
sealed trait FrameworkState
case object Starting extends FrameworkState //waiting for subscription to start
case object Subscribed extends FrameworkState //continue checking heartbeats
case object Exiting extends FrameworkState //do not check heartbeats

case class TaskDef(taskId: String,
                   taskName: String,
                   dockerImage: String,
                   cpus: Double,
                   mem: Int,
                   ports: Seq[Int] = Seq.empty,
                   healthCheckParams: Option[HealthCheckConfig] = None,
                   forcePull: Boolean = false,
                   network: Network = Bridge,
                   dockerRunParameters: Map[String, Set[String]] = Map.empty,
                   commandDef: Option[CommandDef] = None,
                   constraints: Set[Constraint] = Set.empty)

case class HealthCheckConfig(healthCheckPortIndex: Int,
                             delay: Double = 0,
                             interval: Double = 1,
                             timeout: Double = 1,
                             gracePeriod: Double = 25,
                             maxConsecutiveFailures: Int = 3)

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
case class Failed(taskId: String, agentId: String) extends TaskState

case class AgentStats(mem: Double, cpu: Double, lastSeen: Instant)

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
  val refuseSeconds: Double
  var reconcilationData: mutable.Map[String, ReconcileTaskState] = mutable.Map()
  var heartbeatTimeout: FiniteDuration = 60.seconds //will be reset by Subscribed message
  val heartbeatMaxFailures: Int
  var heartbeatMonitor: Option[Cancellable] = None
  var heartbeatFailures: Int = 0
  var agentOfferHistory = Map.empty[String, AgentStats] //track the most recent offer stats per agent

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

  //cache the framework id, so that in case this actor restarts we can reconnect
  if (MesosClient.frameworkID.isEmpty) MesosClient.frameworkID = Some(FrameworkID.newBuilder().setValue(id()).build())
  private val frameworkID = MesosClient.frameworkID.get

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
            case SubmitPending(taskDef, promise) => {
              log.info(s"deleting unlaunched task ${taskDef.taskId}")
              tasks.remove(taskDef.taskId)
            }
            case Submitted(_, taskInfo: TaskInfo, offer: OfferID, _, _, promise) => {
              val del = DeletePending(taskId, deletePromise)
              tasks.update(taskId, del)
              log.info(s"killing submitted task ${taskID.getValue}")
              kill(taskID, taskInfo.getAgentId)
            }
            case Running(taskId, _, taskStatus, _, _) => {
              val del = DeletePending(taskId, deletePromise)
              tasks.update(taskId, del)
              log.info(s"killing running task ${taskID.getValue}")
              kill(taskID, taskStatus.getAgentId)
            }
            case s => {
              log.info(s"deleting task ${taskID.getValue} in state $s")
              tasks.remove(taskID.getValue)
            }
          }

        case None =>
          deletePromise.failure(new MesosException(s"no task was running with id ${taskId}"))
      }
      deletePromise.future.pipeTo(sender())
    }
    case Teardown         => teardown.pipeTo(sender())
    case Reconcile(tasks) => reconcile(tasks)

    //event messages
    case event: Event.Update     => handleUpdate(event)
    case event: Event.Offers     => handleOffers(event)
    case event: Event.Subscribed => handleSubscribed(event)
    case Heartbeat               => handleHeartbeat()

    case msg => log.warning(s"unknown msg: ${msg}")
  }

  def handleUpdate(event: Event.Update) = {
    val taskId = event.getStatus.getTaskId.getValue
    val oldTaskDetails = tasks.get(taskId)
    log.info(
      s"received update for task ${event.getStatus.getTaskId.getValue} in state ${event.getStatus.getState} with previous state ${oldTaskDetails
        .map(_.getClass)}")

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
              new MesosException(s"task in state ${event.getStatus.getState} msg: ${event.getStatus.getMessage}"))
        }
      }
      case Some(r: Running) => {
        event.getStatus.getState match {
          case MesosTaskState.TASK_FAILED | MesosTaskState.TASK_DROPPED | MesosTaskState.TASK_GONE |
              MesosTaskState.TASK_KILLED | MesosTaskState.TASK_LOST => {
            log.error(
              s"task ${event.getStatus.getTaskId.getValue} unexpectedly changed from TASK_RUNNING to ${toCompactJsonString(
                event.getStatus)}")
            val listener = sender() //TODO: allow client to provide the listener
            listener ! Failed(taskId, event.getStatus.getAgentId.getValue)
            tasks.remove(taskId)
          }
          case _ => {
            log.info(
              s"task ${event.getStatus.getTaskId.getValue} changed from TASK_RUNNING to ${toCompactJsonString(event.getStatus)}")
            tasks.update(event.getStatus.getTaskId.getValue, r)
          }
        }
      }
      case Some(DeletePending(taskInfo, promise)) => {
        event.getStatus.getState match {
          case MesosTaskState.TASK_KILLED =>
            log.info(s"successfully killed task ${event.getStatus.getTaskId.getValue}")
            promise.success(Deleted(taskId, event.getStatus))
            tasks.remove(taskId)
          case MesosTaskState.TASK_RUNNING | MesosTaskState.TASK_KILLING | MesosTaskState.TASK_STAGING |
              MesosTaskState.TASK_STARTING =>
            log.info(s"still killing task ${event.getStatus.getTaskId.getValue} (in state ${event.getStatus.getState}")
          case _ =>
            log.warning(
              s"task ended in unexpected state ${event.getStatus.getState} msg: ${toCompactJsonString(event)}")
            promise.failure(
              new MesosException(
                s"task ended in unexpected state ${event.getStatus.getState} msg: ${toCompactJsonString(event)}"))
            tasks.remove(taskId)
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
      case None =>
        log.warning(s"received update for unknown task ${toCompactJsonString(event)}")
      case _ =>
        log.warning(s"unexpected task status ${oldTaskDetails} for update event ${toCompactJsonString(event)}")
        tasks.foreach(t => log.debug(s"      ${t}"))

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

  override def postStop(): Unit = {
    logger.info("postStop cancelling heartbeatMonitor")
    heartbeatMonitor.foreach(_.cancel())
    super.postStop()
  }
  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    logger.warning(s"preRestart restarting ${frameworkID.getValue} after $reason from message $message")
    super.preRestart(reason, message)
  }
  override def postRestart(reason: Throwable): Unit = {
    if (MesosClient.frameworkState == Subscribed) {
      logger.warning(s"postRestart resubscribing ${frameworkID.getValue} after $reason")
      subscribe(frameworkID, frameworkName, failoverTimeoutSeconds.toSeconds)
    }
    super.postRestart(reason)
  }

  def handleOffers(event: Event.Offers) = {

    log.info(s"received ${event.getOffersList.size} offers: ${toCompactJsonString(event);}")

    //store a reference of last memory offer (total) from each agent
    updateAgentStats(event)

    val matchedTasks = taskMatcher.matchTasksToOffers(role, pending, event.getOffersList.asScala.toList, taskBuilder)

    val matchedCount = matchedTasks.foldLeft(0)(_ + _._2.size)
    log.info(s"matched ${matchedCount} tasks to ${matchedTasks.size} offers out of ${pending.size} pending tasks")
    pending.foreach(reqs => {
      log.debug(s"pending task: ${reqs.taskId}")
    })
    matchedTasks.foreach(taskInfos => {
      log.debug(s"using offer ${taskInfos._1.getValue}")
      taskInfos._2.foreach(taskInfo => {
        log.debug(s"     matched task: ${taskInfo._1.getTaskId.getValue}")
      })
    })

    //if no tasks matched, we have to explicitly decline all offers

    //if some tasks matched, we explicitly accept the matched offers, and others are implicitly declined
    if (matchedTasks.isEmpty) {
      val offerIds = asScalaBuffer(event.getOffersList).map(offer => offer.getId)

      val declineCall = Call.newBuilder
        .setFrameworkId(frameworkID)
        .setType(Call.Type.DECLINE)
        .setDecline(
          Call.Decline.newBuilder
            .addAllOfferIds(seqAsJavaList(offerIds))
            .setFilters(Filters.newBuilder().setRefuseSeconds(refuseSeconds)))
        .build;

      execInternal(declineCall)

    } else {
      matchedTasks.foreach { offerTasks =>
        val taskInfos: java.lang.Iterable[TaskInfo] = offerTasks._2.map(_._1).asJava
        val acceptCall = MesosClient.accept(frameworkID, Seq(offerTasks._1).asJava, taskInfos)
        execInternal(acceptCall).onComplete {
          case Success(r) =>
            matchedTasks.foreach(entry => {
              entry._2.foreach(task => {
                tasks(task._1.getTaskId.getValue) match {
                  case s @ SubmitPending(reqs, promise) =>
                    //dig the hostname out of the offer whose agent id matches the agent id in the task info
                    val hostname =
                      event.getOffersList.asScala.find(p => p.getAgentId == task._1.getAgentId).get.getHostname
                    tasks.update(reqs.taskId, Submitted(s, task._1, entry._1, hostname, task._2, promise))
                  case previousState =>
                    log.warning(s"submitted a task that was not in SubmitPending? ${previousState}")
                }
              })

            })
            if (!pending.isEmpty) {
              log.warning("still have pending tasks after OFFER + ACCEPT: ")
              pending.foreach(t => log.info(s"pending taskid ${t.taskId}"))
            }
          case Failure(t) => log.error(s"failure ${t}")
        }

      }

    }
  }

  def leastUsedAgent() = agentOfferHistory.toList.maxBy(_._2.mem)._1 //return agent with largest offer < 3min old

  def updateAgentStats(offers: Event.Offers) = {
    val newOfferStats = offers.getOffersList.asScala
      .filter(_.getResourcesList.asScala.exists(_.getRole == role)) //only include agents with offers that include resources for this role
      .map(o =>
        o.getHostname -> AgentStats(
          o.getResourcesList.asScala
            .filter(r => r.getRole == role && r.getName == "mem")
            .foldLeft(0.0)(_ + _.getScalar.getValue),
          o.getResourcesList.asScala
            .filter(r => r.getRole == role && r.getName == "cpus")
            .foldLeft(0.0)(_ + _.getScalar.getValue),
          Instant.now()))

    log.debug(s"new agent offer stats: ${newOfferStats}")
    agentOfferHistory = agentOfferHistory ++ newOfferStats
    log.info(s"agent offer stats: ${agentOfferHistory}")

    //prune stats that are > 3min old
    agentOfferHistory = agentOfferHistory.filter(_._2.lastSeen.isAfter(Instant.now().minusSeconds(180)))
  }

  def pending() = tasks.collect { case (_, submitPending: SubmitPending) => submitPending.reqs }

  def handleHeartbeat() = {
    resetHeartbeatTimeout()
  }

  def resetHeartbeatTimeout() = {
    //cancel existing
    heartbeatMonitor.foreach { monitor =>
      monitor.cancel()
      if (heartbeatFailures > 0) {
        logger.info(s"resetting heartbeat failure counter after ${heartbeatFailures} failures")
      }
      heartbeatFailures = 0
    }
    if (MesosClient.frameworkState == Subscribed) {
      heartbeatMonitor = Some(actorSystem.scheduler.scheduleOnce(heartbeatTimeout) {
        heartbeatFailure()
      })
    }
  }

  def heartbeatFailure(): Unit = {
    heartbeatFailures += 1
    logger.warning(s"heartbeat not detected for ${heartbeatTimeout} (${heartbeatFailures} failures)")
    if (heartbeatFailures >= heartbeatMaxFailures) {
      logger.warning(s"resubscribing after ${heartbeatFailures} ${heartbeatTimeout} heartbeat failures...")
      resetHeartbeatTimeout()
      subscribe(frameworkID, frameworkName, failoverTimeoutSeconds.toSeconds)
    } else if (MesosClient.frameworkState == Subscribed) {
      heartbeatMonitor = Some(actorSystem.scheduler.scheduleOnce(heartbeatTimeout) {
        heartbeatFailure()
      })
    }
  }

  def handleSubscribed(event: Event.Subscribed) = {
    MesosClient.frameworkState = Subscribed
    if (event.hasHeartbeatIntervalSeconds) {
      heartbeatTimeout = event.getHeartbeatIntervalSeconds.seconds + 2.seconds //allow 2 extra seconds for delayed heartbeat
    } else {
      logger.error("no heartbeat interval received during subscribe!")
    }
    //TODO: persist framework id if you want to reconcile on restart
    log.info(s"subscribed; frameworkId is ${event.getFrameworkId.getValue} streamId is ${streamId}")
  }

  def teardown(): Future[TeardownComplete.type] = {
    MesosClient.frameworkState = Exiting
    heartbeatMonitor.foreach(_.cancel())
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
        Future.failed(new MesosException("not successful"))
      } else {
        Future.successful(resp)
      }
    })
  }

  def handleEvent(event: Event)(implicit ec: ExecutionContext): Unit = {
    log.info(s"receiving ${event.getType}")

    event.getType match {
      case Event.Type.OFFERS     => self ! event.getOffers
      case Event.Type.HEARTBEAT  => self ! Heartbeat
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
                  val tasks: TaskStore,
                  val refuseSeconds: Double,
                  val heartbeatMaxFailures: Int)
    extends MesosClientActor
    with MesosClientHttpConnection {}

object MesosClient {
  protected[mesos] var frameworkID: Option[FrameworkID] = None
  protected[mesos] var frameworkState: FrameworkState = Starting

  def props(id: () => String,
            frameworkName: String,
            master: String,
            role: String,
            failoverTimeoutSeconds: FiniteDuration,
            taskMatcher: TaskMatcher = new DefaultTaskMatcher,
            taskBuilder: TaskBuilder = new DefaultTaskBuilder,
            autoSubscribe: Boolean = false,
            taskStore: TaskStore,
            refuseSeconds: Double = 5.0,
            heartbeatMaxFailures: Int = 2): Props =
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
        taskStore,
        refuseSeconds,
        heartbeatMaxFailures))

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
