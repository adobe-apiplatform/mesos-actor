package mesos

import java.net.URI
import java.util.Optional

import akka.NotUsed
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding.Post
import akka.http.scaladsl.model.MediaType.Compressible
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.{ContentType, HttpEntity, HttpRequest, HttpResponse, MediaType}
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshal, Unmarshaller}
import akka.pattern.pipe
import akka.stream.alpakka.recordio.scaladsl.RecordIOFraming
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{ActorMaterializer, Attributes, FlowShape, Inlet, Outlet}
import akka.util.ByteString
import org.apache.mesos.v1.Protos.{AgentID, ExecutorID, FrameworkID, FrameworkInfo, Offer, OfferID, TaskID, TaskInfo, TaskState, TaskStatus}
import org.apache.mesos.v1.scheduler.Protos.Call._
import org.apache.mesos.v1.scheduler.Protos.Event._
import org.apache.mesos.v1.scheduler.Protos.{Call, Event}

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future, Promise}
/**
  * Created by tnorris on 6/4/17.
  */

//control messages
case class SubmitTask(task:TaskReqs)
case class DeleteTask(taskId:String)
case class Reconcile(tasks:Iterable[TaskRecoveryDetail])
case object Subscribe
case object Teardown
//events
case class SubscribeComplete()
case object TeardownComplete
case class TaskRecoveryDetail(taskId:String, agentId:String)
//data
case class TaskReqs(taskId:String, dockerImage:String, cpus:Double, mem:Int, port:Int)
case class TaskDetails(taskInfo:TaskInfo, taskStatus:TaskStatus = null, hostname:String = null)

//TODO: mesos authentication
class MesosClientActor (val id:String, val frameworkName:String, val master:String, val role:String, val taskMatcher:(String, Iterable[TaskReqs], Iterable[Offer], (TaskReqs,Offer, Int) => TaskInfo) => Map[OfferID,Seq[TaskInfo]], val taskBuilder:(TaskReqs,Offer, Int) => TaskInfo)
        extends Actor with ActorLogging {
  implicit val ec:ExecutionContext = context.dispatcher
  implicit val system:ActorSystem = context.system
  implicit val materializer:ActorMaterializer = ActorMaterializer()(system)

  private var streamId:String = null

  private val frameworkID = FrameworkID.newBuilder().setValue(id).build();
  private val cpusPerTask = 0.1
  //TODO: handle redirect to master see https://github.com/mesosphere/mesos-rxjava/blob/d6fd040af3322552012fb3dcf61debb9886adbf3/mesos-rxjava-client/src/main/java/com/mesosphere/mesos/rx/java/MesosClient.java#L167
  private val mesosUri = URI.create(s"${master}/api/v1/scheduler")
  private var pendingTaskInfo:Map[String,TaskReqs] = Map()
  private var pendingTaskPromises:Map[String,Promise[TaskDetails]] = Map()
  private var deleteTaskPromises:Map[String, Promise[TaskDetails]] = Map()
  private var taskStatuses:Map[String, TaskDetails] = Map()
  private var agentHostnames:Map[String,String] = Map()

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
    case event:Update => handleUpdate(event)
    case event:Offers => handleOffers(event)
    case event:Subscribed =>handleSubscribed(event)
    case event:Event => handleHeartbeat(event)

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
        if (event.getStatus.getHealthy){
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
    case None => {
    }
    }
    deleteTaskPromises.get(event.getStatus.getTaskId.getValue) match {
    case Some(promise) => {
      event.getStatus.getState match {
      case TaskState.TASK_KILLED  =>
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
    if (oldTaskDetails != null && oldTaskDetails.taskStatus != null){
      if (oldTaskDetails.taskStatus.getState == TaskState.TASK_RUNNING &&
              newTaskDetails.taskStatus.getState != TaskState.TASK_RUNNING) {
        log.info(s"task ${oldTaskDetails.taskStatus.getTaskId.getValue} changed from TASK_RUNNING ${toCompactJsonString(newTaskDetails.taskStatus)}")
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
      exec(ack).map(resp => {
        if (resp.status.isSuccess()) {
          log.info(s"ack succeeded")
        } else {
          log.warning(s"ack failed! ${resp}")
        }
      })

    }
  }

  import com.google.protobuf.util.JsonFormat

  private def toCompactJsonString(message: com.google.protobuf.Message) =
    JsonFormat.printer.omittingInsignificantWhitespace.print(message)

  def handleOffers(event: Offers) = {

    log.info(s"received ${event.getOffersList.size} offers: ${toCompactJsonString(event);}")

    //update hostnames if needed
    event.getOffersList.asScala.foreach (offer => {
      val agentID = offer.getAgentId.getValue
      val agentHostname = offer.getHostname
      if (agentHostnames.getOrElse(agentID, "") != agentHostname){
        log.info(s"noticed a new agent (or new hostname for existing agent) ${agentID} ${agentHostname}")
        agentHostnames += (agentID -> agentHostname)
      }
    })

    val matchedTasks = taskMatcher(
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

      exec(declineCall)
              .map(resp => {
                if (resp.status.isSuccess()) {
                  log.info(s"decline succeeded")
                } else {
                  log.warning("failed!")
                }
              })
    } else {
      val acceptCall = MesosClient.accept(frameworkID,
        matchedTasks.keys.asJava,
        matchedTasks.values.flatten.asJava)

      exec(acceptCall)
              .map(resp => {
                if (resp.status.isSuccess()) {
                  log.info(s"accept succeeded")
                } else {
                  log.warning(s"accept failed! ${resp}")
                }
              })
      //todo: verify success

      matchedTasks.values.flatten.map(task => {
        pendingTaskInfo -= task.getTaskId.getValue
        taskStatuses += (task.getTaskId.getValue -> TaskDetails(task))
      })

    }




    if (!pendingTaskInfo.isEmpty){
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


  val connectionFlow: Flow[HttpRequest, HttpResponse, Future[Http.OutgoingConnection]] = {
    Http().outgoingConnection(host = mesosUri.getHost, port = mesosUri.getPort)
  }

  def exec(call:Call): Future[HttpResponse] = {
    val req = Post("/api/v1/scheduler")
            .withHeaders(
              RawHeader("Mesos-Stream-Id", streamId))
            .withEntity(MesosClient.protobufContentType, call.toByteArray)


    Source.single(req)
            .via(connectionFlow)
            .runWith(Sink.head)
  }
  def teardown():Future[TeardownComplete.type] = {
    log.info("submitting teardown message...")
    val teardownCall = Call.newBuilder
            .setFrameworkId(frameworkID)
            .setType(Call.Type.TEARDOWN)
            .build;

    //todo: wait for teardown...
    exec(teardownCall)
            .map(resp => {
              if (resp.status.isSuccess()) {
                log.info(s"teardown succeeded")
              } else {
                log.error(s"teardown failed! ${resp}")
              }
              TeardownComplete
            })
  }

  def revive():Unit = {
    log.info("submitting revive message...")
    val reviveCall = Call.newBuilder()
            .setFrameworkId(frameworkID)
            .setType(Call.Type.REVIVE)
            .build()
    exec(reviveCall)
            .map(resp => {
              if (resp.status.isSuccess()) {
                log.info(s"revive succeeded")
              } else {
                log.error(s"revive failed! ${resp}")
              }
            })
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
    exec(killCall)
            .map(resp => {
              if (resp.status.isSuccess()) {
                log.info(s"kill succeeded")
              } else {
                log.error(s"kill failed! ${resp}")
              }
            })
  }

  def shutdown(executorID:ExecutorID, agentID: AgentID): Unit = {
    val shutdownCall = Call.newBuilder()
            .setFrameworkId(frameworkID)
            .setType(Call.Type.SHUTDOWN)
            .setShutdown(Call.Shutdown.newBuilder()
                    .setExecutorId(executorID)
                    .setAgentId(agentID)
                    .build()
            ).build()
    exec(shutdownCall)
  }

  //TODO: implement
  //def message
  //def request

  def reconcile(tasks:Iterable[TaskRecoveryDetail]):Unit = {

    val reconcile = Call.Reconcile.newBuilder()

    tasks.map(task => {
      reconcile.addTasks(Call.Reconcile.Task.newBuilder()
              .setTaskId(TaskID.newBuilder().setValue(task.taskId))
              .setAgentId(AgentID.newBuilder().setValue(task.agentId))  )
    })

    val reconcileCall = Call.newBuilder()
            .setFrameworkId(frameworkID)
            .setType(Call.Type.RECONCILE)
            .setReconcile(reconcile)
            .build()
    exec(reconcileCall)
  }

  def subscribe(mesosClientActor:ActorRef, frameworkID:FrameworkID, frameworkName:String):Future[SubscribeComplete] = {


    import EventStreamUnmarshalling._

    val result = Promise[SubscribeComplete]

    val subscribeCall = Call.newBuilder()
            .setType(Call.Type.SUBSCRIBE)
            .setFrameworkId(frameworkID)
            .setSubscribe(Call.Subscribe.newBuilder
                    .setFrameworkInfo(FrameworkInfo.newBuilder
                            .setId(frameworkID)
                            .setUser(Optional.ofNullable(System.getenv("user")).orElse("root")) // https://issues.apache.org/jira/browse/MESOS-3747
                            .setName(frameworkName)
                            .setFailoverTimeout(0)
                            //.setRole(role)
                            .build)
                    .build())
            .build()


    //TODO: handle connection failures: http://doc.akka.io/docs/akka-http/10.0.5/scala/http/low-level-server-side-api.html
    //see https://gist.github.com/ktoso/4dda7752bf6f4393d1ac
    //see https://tech.zalando.com/blog/about-akka-streams/?gh_src=4n3gxh1
    Http()
            .singleRequest(Post(s"${mesosUri}/subscribe")
                    .withHeaders(RawHeader("Accept", "application/x-protobuf"),
                      RawHeader("Connection", "close"))
                    .withEntity(MesosClient.protobufContentType, subscribeCall.toByteArray))
            .flatMap(response => {
              if (response.status.isSuccess()){
                log.debug(s"response: ${response} ")
                val newStreamId = response.getHeader("Mesos-Stream-Id").get().value()
                if (streamId == null){
                  log.info(s"setting new streamId ${newStreamId}")
                  streamId = newStreamId
                  result.success(SubscribeComplete())
                } else if (streamId != newStreamId){
                  //TODO: do we need to handle StreamId changes?
                  log.warning(s"streamId has changed! ${streamId}  ${newStreamId}")
                }
                Unmarshal(response).to[Source[Event, NotUsed]]
              } else {
                //TODO: reconnect?
                throw new Exception(s"subscribe response failed: ${response}")
              }
            })

            .foreach(eventSource => {
              eventSource.runForeach(event => {
                handleEvent(event)
              })
            })
    def handleEvent(event: Event)(implicit ec: ExecutionContext) = {
      event.getType match {
      case Event.Type.OFFERS => mesosClientActor ! event.getOffers
      case Event.Type.HEARTBEAT => mesosClientActor ! event
      case Event.Type.SUBSCRIBED => mesosClientActor ! event.getSubscribed
      case Event.Type.UPDATE => mesosClientActor ! event.getUpdate
      case eventType => log.warning(s"unhandled event ${toCompactJsonString(event)}")
        //todo: handle other event types
      }
    }
    result.future
  }

}
object MesosClientActor {
  def props(id:String, name:String, master:String, role:String,
    taskMatcher: (String, Iterable[TaskReqs], Iterable[Offer], (TaskReqs, Offer, Int) => TaskInfo) => Map[OfferID,Seq[TaskInfo]] = MesosClient.defaultTaskMatcher,
    taskBuilder: (TaskReqs, Offer, Int) => TaskInfo): Props =
    Props(new MesosClientActor(id, name, master, role, taskMatcher, taskBuilder))
}

object MesosClient {

  val protobufContentType = ContentType(MediaType.applicationBinary("x-protobuf", Compressible, "proto"))


  //TODO: allow task persistence/reconcile

  val defaultTaskMatcher: (String, Iterable[TaskReqs], Iterable[Offer], (TaskReqs, Offer, Int) => TaskInfo) => Map[OfferID,Seq[TaskInfo]] =
    (role:String, t: Iterable[TaskReqs], o: Iterable[Offer], builder:(TaskReqs, Offer, Int) => TaskInfo) => {
      //we can launch many tasks on a single offer

      var tasksInNeed:ListBuffer[TaskReqs] = t.to[ListBuffer]
      var result = Map[OfferID,Seq[TaskInfo]]()
      var portIndex = 0
      var acceptedOfferAgent:String = null//accepted offers must reside on single agent: https://github.com/apache/mesos/blob/master/src/master/validation.cpp#L1768
      o.map(offer => {

        //TODO: manage explicit and default roles, similar to https://github.com/mesos/kafka/pull/103/files

        val hasSomePorts = offer.getResourcesList.asScala
                .filter(res => res.getName == "ports").size > 0
        if (!hasSomePorts) {
          //TODO: log info about skipping due to lack of ports...
        }

        val agentId = offer.getAgentId.getValue
        if (hasSomePorts && (acceptedOfferAgent == null || acceptedOfferAgent == agentId)){
          acceptedOfferAgent = agentId
          val resources = offer.getResourcesList.asScala
                  .filter(_.getRole == role) //ignore resources with other roles
                  .filter(res => Seq("cpus", "mem").contains(res.getName))
                  .groupBy(_.getName)
                  .mapValues(resources => {
                    resources.iterator.next().getScalar.getValue
                  })
          if (resources.size == 2) {
            var remainingOfferCpus = resources("cpus")
            var remainingOfferMem = resources("mem")
            var acceptedTasks = ListBuffer[TaskInfo]()
            tasksInNeed.map(task => {

              val taskCpus = task.cpus
              val taskMem = task.mem

              //check for a good fit
              if (remainingOfferCpus > taskCpus &&
                      remainingOfferMem > taskMem) {
                remainingOfferCpus -= taskCpus
                remainingOfferMem -= taskMem
                //move the task from InNeed to Accepted

                acceptedTasks += builder(task, offer, portIndex)
                portIndex += 1
                tasksInNeed -= task
              }
            })
            if (!acceptedTasks.isEmpty){
              result += (offer.getId -> acceptedTasks)
            }
          }

        } else {
          //log.info("ignoring offers for other slaves for now")
        }
        result
      })
      result
    }








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

object EventStreamUnmarshalling extends EventStreamUnmarshalling

trait EventStreamUnmarshalling {

  implicit final val fromEventStream: FromEntityUnmarshaller[Source[Event, NotUsed]] = {
    val eventParser = new ServerSentEventParser()

    def unmarshal(entity: HttpEntity) =

      entity.withoutSizeLimit.dataBytes
              .via(RecordIOFraming.scanner())
              .via(eventParser)
              .mapMaterializedValue(_ => NotUsed: NotUsed)

    Unmarshaller.strict(unmarshal).forContentTypes(MesosClient.protobufContentType)
  }
}


private final class ServerSentEventParser() extends GraphStage[FlowShape[ByteString, Event]] {

  override val shape =
    FlowShape(Inlet[ByteString]("ServerSentEventParser.in"), Outlet[Event]("ServerSentEventParser.out"))

  override def createLogic(attributes: Attributes) =
    new GraphStageLogic(shape) with InHandler with OutHandler {

      import shape._

      setHandlers(in, out, this)

      override def onPush() = {
        val line: ByteString = grab(in)
        //unmarshall proto
        val event = Event.parseFrom(line.toArray)
        push(out, event)
      }

      override def onPull() = pull(in)
    }
}