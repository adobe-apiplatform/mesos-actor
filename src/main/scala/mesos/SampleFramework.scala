package mesos

import java.time.Instant
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.{ActorSystem, CoordinatedShutdown}
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.util.Timeout
import org.apache.mesos.v1.Protos.ContainerInfo.DockerInfo
import org.apache.mesos.v1.Protos.ContainerInfo.DockerInfo.PortMapping
import org.apache.mesos.v1.Protos.Value.Ranges
import org.apache.mesos.v1.Protos.{CommandInfo, ContainerInfo, Environment, Offer, Resource, TaskID, TaskInfo, TaskStatus, Value}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

/**
  * Created by tnorris on 6/5/17.
  */
class SampleFramework {

}

object SampleFramework {


  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("whisk-framework-system")
    implicit val mat = ActorMaterializer()
    implicit val log = system.log
    implicit val ec = system.dispatcher

    val taskLaunchTimeout = Timeout(15 seconds)
    val taskDeleteTimeout = Timeout(10 seconds)
    val subscribeTimeout = Timeout(5 seconds)
    val teardownTimeout = Timeout(5 seconds)



    //TODO: generate + store a unique id
    val mesosClientActor = system.actorOf(MesosClientActor.props(
      "whisk-invoker-"+UUID.randomUUID(),
      "whisk-framework",
      "http://192.168.99.100:5050",
      "*",
      taskBuilder = buildTask
    ))

    //mesosClientActor ! Subscribe

    mesosClientActor.ask(Subscribe)(subscribeTimeout).mapTo[SubscribeComplete].onComplete(complete => {
      log.info("subscribe completed successfully...")
    })

    val taskId = s"task-0-${Instant.now.getEpochSecond}"

    val task = TaskReqs(taskId, 0.1, 24, 8080)


    val launched:Future[TaskDetails] = mesosClientActor.ask(SubmitTask(task))(taskLaunchTimeout).mapTo[TaskDetails]

    launched map { taskDetails =>
//      val taskHost = taskDetails.taskStatus.getContainerStatus.getNetworkInfos(0).getIpAddresses(0)
      val taskHost = taskDetails.hostname
      val taskPort = taskDetails.taskInfo.getResourcesList.asScala.filter(_.getName == "ports").iterator.next().getRanges.getRange(0).getBegin.toInt
      log.info(s"launched task with state ${taskDetails.taskStatus.getState} on host:port ${taskHost}:${taskPort}")

      //schedule delete in 10 seconds
      system.scheduler.scheduleOnce(Duration.create(40, TimeUnit.SECONDS),
        () => {
          mesosClientActor.ask(DeleteTask(taskId))(taskDeleteTimeout).mapTo[TaskStatus].map(taskStatus => {
            log.info(s"task killed ended with state ${taskStatus.getState}")
          })
        }
      )
    } recover {
      case t => log.error(s"task launch failed ${t.getMessage}", t)
    }




    //handle shutdown
    CoordinatedShutdown(system).addJvmShutdownHook {
      println("custom JVM shutdown hook...")
      val complete:Future[Any] = mesosClientActor.ask(Teardown)(teardownTimeout)
      val result = Await.result(complete, 10.seconds)
      log.info("teardown completed!")
    }

  }
  def buildTask(reqs:TaskReqs, offer:Offer):TaskInfo = {
    val containerPort = reqs.port
    val hostPort = offer.getResourcesList.asScala
      .filter(res => res.getName == "ports").iterator.next().getRanges.getRange(0).getBegin.toInt
    val agentHost = offer.getHostname
    val dockerImage = "trinitronx/python-simplehttpserver"

    val task = TaskInfo.newBuilder
      .setName("test")
      .setTaskId(TaskID.newBuilder
        .setValue(reqs.taskId))
      .setAgentId(offer.getAgentId)
      .setCommand(CommandInfo.newBuilder
        .setEnvironment(Environment.newBuilder
          .addVariables(Environment.Variable.newBuilder
            .setName("__OW_API_HOST")
            .setValue(agentHost)))
        .setShell(false)
        .build())
      .setContainer(ContainerInfo.newBuilder
        .setType(ContainerInfo.Type.DOCKER)
        .setDocker(DockerInfo.newBuilder
          .setImage(dockerImage)
          .setNetwork(DockerInfo.Network.BRIDGE)
          .addPortMappings(PortMapping.newBuilder
            .setContainerPort(containerPort)
            .setHostPort(hostPort)
            .build)
        ).build())
        .addResources(Resource.newBuilder()
          .setName("ports")
          .setType(Value.Type.RANGES)
          .setRanges(Ranges.newBuilder()
              .addRange(Value.Range.newBuilder()
                .setBegin(hostPort)
                .setEnd(hostPort))))
      .addResources(Resource.newBuilder
        .setName("cpus")
        .setRole("*")
        .setType(Value.Type.SCALAR)
        .setScalar(Value.Scalar.newBuilder
          .setValue(reqs.cpus)))
      .addResources(Resource.newBuilder
        .setName("mem")
        .setRole("*")
        .setType(Value.Type.SCALAR)
        .setScalar(Value.Scalar.newBuilder
          .setValue(reqs.mem)))
      .build
    task
  }
}
