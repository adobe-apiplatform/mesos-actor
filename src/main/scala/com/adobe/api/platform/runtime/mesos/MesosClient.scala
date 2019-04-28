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

import akka.Done
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Cancellable
import akka.actor.CoordinatedShutdown
import akka.actor.Props
import akka.dispatch.Futures
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
import org.apache.mesos.v1.Protos.FrameworkID
import org.apache.mesos.v1.Protos.Offer
import org.apache.mesos.v1.Protos.OfferID
import org.apache.mesos.v1.Protos.Resource
import org.apache.mesos.v1.Protos.TaskID
import org.apache.mesos.v1.Protos.TaskInfo
import org.apache.mesos.v1.Protos.TaskStatus
import org.apache.mesos.v1.Protos.TaskStatus.Reason
import org.apache.mesos.v1.Protos.{TaskState => MesosTaskState}
import org.apache.mesos.v1.scheduler.Protos.Call
import org.apache.mesos.v1.scheduler.Protos.Call._
import org.apache.mesos.v1.scheduler.Protos.Event
import pureconfig._
import pureconfig.loadConfigOrThrow
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.Buffer
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
case class TaskRecoveryLaunchDetail(taskId: String)

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
case class SubmitPending(reqs: TaskDef, promise: Promise[Running], offerCycles: Int = 1) extends TaskState
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

case class MesosActorConfig(agentStatsTTL: FiniteDuration,
                            agentStatsPruningPeriod: FiniteDuration,
                            failPendingOfferCycles: Option[Int],
                            holdOffers: Boolean,
                            holdOffersTTL: FiniteDuration,
                            holdOffersPruningPeriod: FiniteDuration,
                            waitForPreferredAgent: Boolean)
case class AgentStats(mem: Double, cpu: Double, ports: Int, expiration: Instant)

case class MesosAgentStats(stats: Map[String, AgentStats])

case class CapacityFailure(requiredMem: Float,
                           requiredCpu: Float,
                           requiredPorts: Int,
                           remainingResources: List[(Float, Float, Int)])
    extends MesosException("cluster does not have capacity")

case class DockerLaunchFailure(msg: String) extends MesosException(s"docker launch failed: $msg")

case class HeldOffer(offer: Offer, expiration: Instant)

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
  var heldOffers: Map[OfferID, HeldOffer] = Map.empty
  var waitForAgent
    : Option[String] = None //when set, offers will be skipped until we see an offer from this specific agent, OR some offers are reaching their max offer cycles
  var pendingHeldOfferMatch: Boolean = false
  var stopping: Boolean = false

  var agentOfferHistory = Map.empty[String, AgentStats] // Map[<agent hostname> -> <stats>] track the most recent offer stats per agent hostname
  val listener: Option[ActorRef]

  val config: MesosActorConfig

  var portsBlacklist: Map[String, Seq[Int]] = Map.empty

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
        //we also may have unlaunched tasks that should proceed with launch as part of autosubscribe process
        val toLaunchAtReconcile = tasks.reconcilePendingData
        toLaunchAtReconcile.foreach { t =>
          self ! SubmitTask(t._2.taskDef)
        }
      })
  }
  case object PruneStats
  case object PruneHeldOffers
  case object ReleaseHeldOffers
  case object MatchHeldOffers //used when submitting tasks, and periodically to refresh nodestats based on held offers

  override def preStart() = {
    actorSystem.scheduler.schedule(30.seconds, config.agentStatsPruningPeriod, self, PruneStats)
    if (config.holdOffers) {
      actorSystem.scheduler.schedule(30.seconds, config.holdOffersPruningPeriod, self, PruneHeldOffers)
      //setup shutdown hook for releasing held offers
      CoordinatedShutdown(actorSystem).addTask(CoordinatedShutdown.PhaseBeforeServiceUnbind, "releaseOffers") { () =>
        stopping = true
        implicit val timeout = Timeout(5.seconds)
        (self ? ReleaseHeldOffers).map(_ => Done)
      }
    }
  }
  //cache the framework id, so that in case this actor restarts we can reconnect
  if (MesosClient.frameworkID.isEmpty) MesosClient.frameworkID = Some(FrameworkID.newBuilder().setValue(id()).build())
  private val frameworkID = MesosClient.frameworkID.get

  //TODO: handle redirect to master see https://github.com/mesosphere/mesos-rxjava/blob/d6fd040af3322552012fb3dcf61debb9886adbf3/mesos-rxjava-client/src/main/java/com/mesosphere/mesos/rx/java/MesosClient.java#L167
  val mesosUri = URI.create(s"${master}/api/v1/scheduler")
  var streamId: String = null
  var subscribed: Option[Future[SubscribeComplete]] = None

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
      //if we are allowed to hold offers, signal use of those immediately, but only if not already signaled
      if (config.holdOffers && taskFitsHeldOffers(task) && !pendingHeldOfferMatch) {
        pendingHeldOfferMatch = true
        //don't do offer matching here, or send the held offers (which may go away), rather trigger a separate offer cycle
        //that only uses the held offers
        self ! MatchHeldOffers
      }
      taskPromise.future.pipeTo(sender())
    }
    case DeleteTask(taskId) => {
      val taskID = TaskID.newBuilder().setValue(taskId).build()
      val deletePromise = Promise[Deleted]()

      tasks.get(taskID.getValue) match {
        case Some(taskDetails) =>
          taskDetails match {
            case SubmitPending(taskDef, promise, _) => {
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
    case PruneStats              =>
      //prune stats that are > TTL old (remove old/unseen agents' data)
      val toPrune = agentOfferHistory.filter(_._2.expiration.isBefore(Instant.now()))
      if (toPrune.nonEmpty) {
        agentOfferHistory = agentOfferHistory -- toPrune.keys
        logger.info(s"pruned ${toPrune.size} expired agent stats")
        //publish MesosAgentStats to subscribers
        listener.foreach(_ ! MesosAgentStats(agentOfferHistory))
      }
    case PruneHeldOffers =>
      //prune one offers that are > TTL old (will remove and trigger a new offer, so don't prune all at once)
      val now = Instant.now()
      val expired = heldOffers.filter(_._2.expiration.isBefore(now))
      if (expired.nonEmpty) {
        //prune only the oldest one per pruning (so that we retain some held offers at all times, if possible)
        val expiredOffer = expired.minBy(_._2.expiration)._1
        declineOffers(Seq(expiredOffer))
        heldOffers = heldOffers - expiredOffer
        logger.info(s"pruned 1 held offers")
      }
    case ReleaseHeldOffers =>
      if (heldOffers.nonEmpty) {
        logger.info(s"declining (on shutdown) ${heldOffers.size} held offers")
        val declineCall = Call.newBuilder
          .setFrameworkId(frameworkID)
          .setType(Call.Type.DECLINE)
          .setDecline(
            Call.Decline.newBuilder
              .addAllOfferIds(heldOffers.keys.asJava)
              .build)
          .build()
        execInternal(declineCall).pipeTo(sender())
        heldOffers = Map.empty
      } else {
        logger.info("no offers held (on shutdown)")
        sender() ! Future.successful({})
      }
    case MatchHeldOffers =>
      pendingHeldOfferMatch = false //reset flag to allow signaling again (would be better as FSM)
      //use held offers IFF we have held offers AND either a) matching waitForAgent or b) empty waitForAgent
      if (heldOffers.nonEmpty && waitForAgent.forall(h => heldOffers.values.map(_.offer.getHostname).exists(_ == h))) {
        log.info(s"attempting to use ${heldOffers.size} held offers")
        //send empty offer list (held offers will be merged within handleOffers)
        handleOffers(Event.Offers.getDefaultInstance)
      }
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
          case MesosTaskState.TASK_FAILED if event.getStatus.getMessage == MesosClient.DOCKER_RUN_FAILURE_MESSAGE =>
            log.warning(toCompactJsonString(event))
            val port = hostports.headOption.getOrElse(0)
            log.warning(
              s"docker run failed for task ${event.getStatus.getTaskId.getValue} at $hostname:$port; port will be blacklisted; msg: ${event.getStatus.getMessage}")
            //update the blacklist
            val hostPortBlacklist = portsBlacklist.getOrElse(hostname, Seq.empty) :+ port
            portsBlacklist = portsBlacklist + (hostname -> hostPortBlacklist)
            log.warning(s"port blacklist is ${portsBlacklist}")
            //remove the old task
            tasks.remove(event.getStatus.getTaskId.getValue)
            //TODO: don't allow launching on this allocated port for some time
            promise.failure(new DockerLaunchFailure(event.getStatus.getMessage))
          case t =>
            log.warning(
              s"failing task ${event.getStatus.getTaskId.getValue} exception: ${t}  msg: ${event.getStatus.getMessage}")
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
  def taskFitsHeldOffers(task: TaskDef): Boolean = {
    taskMatcher.matchTasksToOffers(role, Seq(task), heldOffers.map(_._2.offer), taskBuilder, portsBlacklist)._1.nonEmpty
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
  def declineOffers(offerIds: Iterable[OfferID]) = {
    if (offerIds.nonEmpty) {
      val declineCall = Call.newBuilder
        .setFrameworkId(frameworkID)
        .setType(Call.Type.DECLINE)
        .setDecline(Call.Decline.newBuilder
          .addAllOfferIds(offerIds.asJava))
        .build;
      execInternal(declineCall).onComplete {
        case Success(_) => log.debug(s"successfully declined ${offerIds.size} offers")
        case Failure(t) => log.error(s"decline failed ${t}")
      }
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

  def handleOffers(newOffers: Event.Offers) = {
    //merge held offers into any offers just received
    val event =
      if (heldOffers.nonEmpty) {
        log.info(s"including ${heldOffers.size} held offers ")
        val heldOffersEvent = Event.Offers.newBuilder().addAllOffers(heldOffers.map(_._2.offer).asJava).build()
        Event.Offers
          .newBuilder()
          .addAllOffers(newOffers.getOffersList)
          .addAllOffers(heldOffersEvent.getOffersList)
          .build()
      } else {
        newOffers
      }

    val agentOfferMap = event.getOffersList.asScala
      .map(o => o -> o.getResourcesList.asScala.filter(_.getRole == role).groupBy(_.getName)) //map hostname to resource map including only resources allocated to this role
      .filter(a => Set("cpus", "mem", "ports").subsetOf(a._2.keySet)) //remove hosts that do not have resources allocated for all of cpus+mem+ports
      .toMap
    if (agentOfferMap.nonEmpty) {
      log.info(
        s"received ${event.getOffersList.size} offers total and ${agentOfferMap.size} usable offers for role $role")
      agentOfferMap.foreach(o =>
        log.info(
          s"usable offer ${o._1.getHostname}: max mem:${o._2("mem").maxBy(_.getScalar.getValue).getScalar.getValue}  max cpus:${o
            ._2("cpus")
            .maxBy(_.getScalar.getValue)
            .getScalar
            .getValue} max ports:${MesosClient.countPorts(o._2("ports"))}"))

      log.debug(s"agent offer stats: {}", agentOfferHistory)

      //if new offers include agent that we are waiting for, clear the wait state
      waitForAgent.foreach { w =>
        if (event.getOffersList.asScala.exists(_.getHostname == w)) {
          log.info(s"resetting waitForAgent since we received an offer on ${w}")
          waitForAgent = None
        } else {
          //if any tasks are close to expiring, clear the wait state
          val countAtLimit = countPendingAtOfferCycleLimit()
          if (countAtLimit > 0) {
            log.info(s"resetting waitForAgent since ${countAtLimit} pending tasks are at offer cycle limit")
            waitForAgent = None
          }
        }
      }

      val (matchedTasks, remaining) = if (pending.nonEmpty && waitForAgent.isEmpty) {
        taskMatcher.matchTasksToOffers(role, pending, event.getOffersList.asScala.toList, taskBuilder, portsBlacklist)
      } else {
        waitForAgent.foreach(w => log.info(s"skipping offers due to waitForAgent ${w}"))
        //TODO: do not return empty map, it may cause errors on maxBy in caller!
        (Map.empty[OfferID, Seq[(TaskInfo, Seq[Int])]], Map.empty[OfferID, (Float, Float, Int)])
      }

      if (matchedTasks.nonEmpty && config.waitForPreferredAgent) {
        val smallestRemainingOfferId = remaining.toSeq.minBy(_._2._1)._1
        waitForAgent = agentOfferMap.keySet.find(_.getId == smallestRemainingOfferId).map(_.getHostname)
        log.info(s"setting waitForAgent to ${waitForAgent}")
      } else {
        //leave waitForAgent unchanged
      }

      val matchedCount = matchedTasks.foldLeft(0)(_ + _._2.size)
      val matchedOrNone = if (matchedCount > 0) "MATCHED" else "NOMATCHES"
      val pendingOrNone = if (pending.size > 0) "PENDING" else "NOPENDINGS"
      log.info(
        s"${matchedOrNone} ${pendingOrNone} matched ${matchedCount} tasks to ${matchedTasks.size} offers out of ${pending.size} pending tasks")

      //debugging logs
      pending.foreach(reqs => {
        log.debug(s"pending task: ${reqs.taskId}")
      })
      matchedTasks.foreach(taskInfos => {
        log.debug(s"using offer ${taskInfos._1.getValue}")
        taskInfos._2.foreach(taskInfo => {
          log.debug(s"     matched task: ${taskInfo._1.getTaskId.getValue}")
        })
      })

      //if some tasks matched, we explicitly accept the matched offers, and others are explicitly declined

      //Decline the offers not selected. Sometimes these just stay dangling in mesos outstanding offers
      val unusedOfferIds =
        asScalaBuffer(event.getOffersList).map(offer => offer.getId).filter(!matchedTasks.contains(_))
      //do not refill heldOffers if we are stopping
      heldOffers = Map.empty
      if (config.holdOffers && !stopping) { //handle held offers
        //add remaining unused offers
        if (unusedOfferIds.nonEmpty && config.holdOffers) {
          val now = Instant.now()
          //find the usable ones but if this offer host is the least used host, do not hold it...
          val usableUnusedOffers =
            agentOfferMap.filter(
              a =>
                unusedOfferIds.contains(a._1.getId) && !matchedTasks.keySet
                  .contains(a._1.getId))
          //we may hold offers that were not accepted, but if we are waiting for a specific agent they wouldn't be used anyways...
          if (usableUnusedOffers.nonEmpty) {
            logger.info(
              s"holding ${usableUnusedOffers.size} unused offers: ${usableUnusedOffers.map(_._1.getHostname)}")
            //save the usable ones
            usableUnusedOffers.foreach(
              o =>
                heldOffers = heldOffers + (o._1.getId -> HeldOffer(
                  o._1,
                  now.plusSeconds(config.holdOffersTTL.toSeconds))))
          }

          //remove the usable from unused
          val unusableUnusedOfferIds = unusedOfferIds -- heldOffers.keys
          //decline unused - usableUnused
          if (unusableUnusedOfferIds.nonEmpty) {
            logger.info(s"declining ${unusableUnusedOfferIds.size} unused or unusable offers")
            declineOffers(unusableUnusedOfferIds)
          }
        }
      } else {
        logger.info(s"declining ${unusedOfferIds.size} unused offers")
        declineOffers(unusedOfferIds)
      }

      matchedTasks.foreach { offerTasks =>
        val taskInfos: java.lang.Iterable[TaskInfo] = offerTasks._2.map(_._1).asJava
        val acceptCall = MesosClient.accept(frameworkID, Seq(offerTasks._1).asJava, taskInfos)
        execInternal(acceptCall).onComplete {
          case Success(r) =>
          //ACCEPT was successful, we will be updated when it changes to RUNNING
          case Failure(t) =>
            log.error(s"accept failure ${t}")
            offerTasks._2.foreach(task => {
              tasks(task._1.getTaskId.getValue) match {
                case s @ Submitted(pending, taskInfo, offer, hostname, hostports, promise) =>
                  promise.failure(t)
                case previousState =>
                  log.warning(s"accepted a task that was not in Submitted? ${previousState}")
              }
            })
        }
        //immediately update tasks to Submitted status
        offerTasks._2.foreach(task => {
          tasks(task._1.getTaskId.getValue) match {
            case s @ SubmitPending(reqs, promise, _) =>
              //dig the hostname out of the offer whose agent id matches the agent id in the task info
              val hostname =
                event.getOffersList.asScala.find(p => p.getAgentId == task._1.getAgentId).get.getHostname
              tasks.update(reqs.taskId, Submitted(s, task._1, offerTasks._1, hostname, task._2, promise))
            case previousState =>
              log.warning(s"submitted a task that was not in SubmitPending? ${previousState}")
          }
        })
      }

      val matchedTaskIds = matchedTasks.values.flatMap(_.map(_._1.getTaskId.getValue)).toSet
      val unmatchedPending = pending.filter(p => !matchedTaskIds.contains(p.taskId))
      if (unmatchedPending.nonEmpty) {
        log.warning("still have pending tasks after OFFER + ACCEPT: ")
        unmatchedPending.foreach(t => log.info(s"pending taskid ${t.taskId} ${t.mem}MB ${t.cpus}CPUS"))
      }
      //generate failures for pending tasks that did not fit any offers
      config.failPendingOfferCycles.foreach { maxOfferCycles =>
        val submitPending = tasks.collect { case (_, s: SubmitPending) => s }
        if (submitPending.nonEmpty) {
          submitPending.foreach { task =>
            if (task.offerCycles > maxOfferCycles) {
              log.warning(s"failing task ${task.reqs.taskId} after ${task.offerCycles} unmatching offer cycles")
              tasks.remove(task.reqs.taskId)
              task.promise.failure(
                new CapacityFailure(
                  task.reqs.mem.toFloat,
                  task.reqs.cpus.toFloat,
                  task.reqs.ports.size,
                  remaining.values.toList))
            } else {
              tasks.update(task.reqs.taskId, task.copy(offerCycles = task.offerCycles + 1)) //increase the offer cycles this task has seen
            }
          }
        }
      }

      //store a reference of last memory offer (total) from each agent
      val newOfferStats = MesosClient.getOfferStats(config, role, agentOfferMap)
      //log the agents that previously had offers, but no longer have offers, and are not yet pruned
      val diff = newOfferStats.keySet.diff(agentOfferMap.keySet.map(_.getHostname))
      if (diff.nonEmpty) {
        log.info(s"some agents not included in offer cycle: ${diff} ")
      }
      //update agentOfferHistory (replace existing agents' data, add new agents' data); not all agents may be included in every offer cycle
      agentOfferHistory = agentOfferHistory ++ newOfferStats
      //publish MesosAgentStats to subscribers
      listener.foreach(_ ! MesosAgentStats(agentOfferHistory))
    } else {
      //decline all offers
      declineOffers(event.getOffersList.asScala.map(_.getId))
      log.info(
        s"received ${event.getOffersList.size} offers; none usable for role $role; ${pending.size} pending tasks")
    }
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
        //remove any held offers
        val rescinded = event.getRescind.getOfferId
        if (heldOffers.keySet.contains(rescinded)) {
          logger.info(s"removing held offer id ${rescinded.getValue} on rescind")
          heldOffers = heldOffers - rescinded
        } else {
          logger.warning(s"unknown offer rescinded ${rescinded.getValue}")
        }
      case Event.Type.FAILURE => logger.warning(s"received failure message ${event.getFailure}")
      case Event.Type.ERROR   => logger.error(s"received error ${event.getError}")
      case eventType          => logger.warning(s"unhandled event ${toCompactJsonString(event)}")

    }
  }
  def toCompactJsonString(message: com.google.protobuf.Message) =
    JsonFormat.printer.omittingInsignificantWhitespace.print(message)

  private def countPendingAtOfferCycleLimit() = {
    config.failPendingOfferCycles
      .map { maxOfferCycles =>
        tasks
          .collect { case (_, s: SubmitPending) => s }
          .count(_.offerCycles > math.max(1, maxOfferCycles - 1))
      }
      .getOrElse(0)
  }
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
                  val heartbeatMaxFailures: Int,
                  val listener: Option[ActorRef],
                  val config: MesosActorConfig)
    extends MesosClientActor
    with MesosClientHttpConnection {}

object MesosClient {
  val DOCKER_RUN_FAILURE_MESSAGE = "Container exited with status 125"
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
            heartbeatMaxFailures: Int = 2,
            listener: Option[ActorRef] = None,
            config: MesosActorConfig = loadConfigOrThrow[MesosActorConfig]("mesos-actor")): Props =
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
        heartbeatMaxFailures,
        listener,
        config))

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
  def getOfferStats(config: MesosActorConfig,
                    role: String,
                    agentOfferMap: Map[Offer, Map[String, Buffer[Resource]]]) = {
    //calculate expiration
    val expiration = Instant.now().plusSeconds(config.agentStatsTTL.toSeconds)
    //ports cannot be mapped to sum, need to calculate the size of ranges
    agentOfferMap.map { o =>
      val hostname = o._1.getHostname
      val resources = o._2
      hostname ->
        AgentStats(
          resources("mem").map(_.getScalar.getValue).sum,
          resources("cpus").map(_.getScalar.getValue).sum,
          countPorts(resources("ports")),
          expiration)
    }
  }

  protected[mesos] def countPorts(portsResources: Buffer[Resource]): Int = {
    portsResources.foldLeft(0)(
      _ + _.getRanges.getRangeList.asScala.foldLeft(0)((a, b) => a + b.getEnd.toInt - b.getBegin.toInt + 1))
  }
}
