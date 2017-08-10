package mesos

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
import org.apache.mesos.v1.Protos.TaskState
import org.apache.mesos.v1.Protos.TaskStatus
import org.apache.mesos.v1.scheduler.Protos.Call
import org.apache.mesos.v1.scheduler.Protos.Call._
import org.apache.mesos.v1.scheduler.Protos.Event
import org.apache.mesos.v1.scheduler.Protos.Event._
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.collection.mutable
import scala.util.Failure
import scala.util.Success

/**
 * Created by tnorris on 6/4/17.
 */

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

case class TaskDetails(taskInfo: TaskInfo, taskStatus: TaskStatus = null, hostname: String = null)

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

    private val pendingTaskInfo: mutable.Map[String, TaskReqs] = mutable.Map()
    private val pendingTaskPromises: mutable.Map[String, Promise[TaskDetails]] = mutable.Map()
    private val deleteTaskPromises: mutable.Map[String, Promise[TaskDetails]] = mutable.Map()
    private val taskStatuses: mutable.Map[String, TaskDetails] = mutable.Map()
    private val agentHostnames: mutable.Map[String, String] = mutable.Map()

    //TODO: FSM for handling subscribing, subscribed, failed, etc states
    override def receive: Receive = {
        //control messages
        case Subscribe => {
            subscribe(self, frameworkID, frameworkName).pipeTo(sender())
        }
        case SubmitTask(task) => {
            val taskPromise = Promise[TaskDetails]()
            pendingTaskPromises += (task.taskId -> taskPromise)
            pendingTaskInfo += (task.taskId -> task)
            taskPromise.future.pipeTo(sender())
        }
        case DeleteTask(taskId) => {
            val taskID = TaskID.newBuilder().setValue(taskId).build()
            val taskPromise = Promise[TaskDetails]()
            taskStatuses.get(taskID.getValue) match {
            case Some(taskDetails) =>
                deleteTaskPromises += (taskID.getValue -> taskPromise)
                kill(taskID, taskDetails.taskStatus.getAgentId)
            case None =>
                taskPromise.failure(new Exception(s"no task was running with id ${taskId}"))
            }
            taskPromise.future.pipeTo(sender())
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

        val oldTaskDetails = taskStatuses(event.getStatus.getTaskId.getValue)
        val newTaskDetailsAgentId = event.getStatus.getAgentId.getValue
        val newTaskDetails = TaskDetails(oldTaskDetails.taskInfo, event.getStatus, agentHostnames.getOrElse(newTaskDetailsAgentId, s"unknown-agent-${newTaskDetailsAgentId}"))
        taskStatuses += (event.getStatus.getTaskId.getValue -> newTaskDetails)
        pendingTaskPromises.get(event.getStatus.getTaskId.getValue) match {
        case Some(promise) => {
            event.getStatus.getState match {
            case TaskState.TASK_RUNNING =>
                log.info(s"received TASK_RUNNING update for ${event.getStatus.getTaskId.getValue} and task health is ${event.getStatus.getHealthy}")
                if (event.getStatus.getHealthy) {
                    promise.success(newTaskDetails)
                    pendingTaskPromises -= event.getStatus.getTaskId.getValue
                }
            case TaskState.TASK_STAGING | TaskState.TASK_STARTING =>
                log.info(s"task still launching task ${event.getStatus.getTaskId.getValue} (in state ${event.getStatus.getState}")
            case _ =>
                log.warning(s"failing task ${event.getStatus.getTaskId.getValue}  msg: ${event.getStatus.getMessage}")
                promise.failure(new Exception(s"task in state ${event.getStatus.getState} msg: ${event.getStatus.getMessage}"))
                pendingTaskPromises -= event.getStatus.getTaskId.getValue
            }
        }
        case None => log.debug(s"no pending promise for task ${event.getStatus.getTaskId.getValue}")
        }
        deleteTaskPromises.get(event.getStatus.getTaskId.getValue) match {
        case Some(promise) => {
            event.getStatus.getState match {
            case TaskState.TASK_KILLED =>
                promise.success(newTaskDetails)
                deleteTaskPromises -= event.getStatus.getTaskId.getValue
            case TaskState.TASK_RUNNING | TaskState.TASK_KILLING | TaskState.TASK_STAGING | TaskState.TASK_STARTING =>
                log.info(s"task still killing task ${event.getStatus.getTaskId.getValue} (in state ${event.getStatus.getState}")
            case _ =>
                deleteTaskPromises -= event.getStatus.getTaskId.getValue
                promise.failure(new Exception(s"task ended in unexpected state ${event.getStatus.getState} msg: ${event.getStatus.getMessage}"))
            }
        }
        case None => {
        }
        }
        //if previous state was TASK_RUNNING, but is no longer, log the details
        if (oldTaskDetails != null && oldTaskDetails.taskStatus != null) {
            if (oldTaskDetails.taskStatus.getState == TaskState.TASK_RUNNING &&
                    newTaskDetails.taskStatus.getState != TaskState.TASK_RUNNING) {
                log.info(s"task ${oldTaskDetails.taskStatus.getTaskId.getValue} changed from TASK_RUNNING to ${toCompactJsonString(newTaskDetails.taskStatus)}")
            }
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

        //update hostnames if needed
        event.getOffersList.asScala.foreach(offer => {
            val agentID = offer.getAgentId.getValue
            val agentHostname = offer.getHostname
            if (agentHostnames.getOrElse(agentID, "") != agentHostname) {
                log.info(s"noticed a new agent (or new hostname for existing agent) ${agentID} ${agentHostname}")
                agentHostnames += (agentID -> agentHostname)
            }
        })

        val matchedTasks = taskMatcher.matchTasksToOffers(
            role,
            pendingTaskInfo.values,
            event.getOffersList.asScala.toList,
            taskBuilder)

        log.info(s"matched ${matchedTasks.size} tasks; ${pendingTaskInfo.size} pending tasks remain")
        pendingTaskInfo.values.foreach(reqs => {
            log.info(s"pending task: ${reqs.taskId}")
        })
        matchedTasks.values.foreach(taskInfos => {
            taskInfos.foreach(taskInfo => {
                log.info(s"     matched task: ${taskInfo.getTaskId.getValue}")
            })
        })

        //if not tasks matched, we have to explicitly decline all offers

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
                    log.info("success")
                    matchedTasks.values.flatten.map(task => {
                        pendingTaskInfo -= task.getTaskId.getValue
                        taskStatuses += (task.getTaskId.getValue -> TaskDetails(task))
                    })
                case Failure(_) => log.info("failure")
            }
        }

        if (!pendingTaskInfo.isEmpty) {
            log.warning("still have pending tasks! (may be oversubscribed)")
        }

    }

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

