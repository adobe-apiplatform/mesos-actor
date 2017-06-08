package mesos

import java.time.Instant
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.actor.{ActorSystem, CoordinatedShutdown}
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.util.Timeout
import org.apache.mesos.v1.Protos.{AgentID, CommandInfo, Environment, Resource, TaskID, TaskInfo, TaskStatus, Value}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
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
    val teardownTimeout = Timeout(5 seconds)



    //TODO: generate + store a unique id
    val mesosClientActor = system.actorOf(MesosClientActor.props(
      "whisk-invoker-"+UUID.randomUUID(),
      "whisk-framework",
      "http://localhost:5050",
      "*"
    ))

    mesosClientActor ! Subscribe

    val taskId = s"task-0-${Instant.now.getEpochSecond}"
    val task = TaskInfo.newBuilder
      .setName("test")
      .setTaskId(TaskID.newBuilder
        .setValue(taskId))
      .setAgentId(AgentID.newBuilder.setValue("fakeid").build())
      .setCommand(CommandInfo.newBuilder
        .setEnvironment(Environment.newBuilder
          .addVariables(Environment.Variable.newBuilder
            .setName("SLEEP_SECONDS")
            .setValue("25")))
        .setValue("env | sort && sleep $SLEEP_SECONDS"))
      .addResources(Resource.newBuilder
        .setName("cpus")
        .setRole("*")
        .setType(Value.Type.SCALAR)
        .setScalar(Value.Scalar.newBuilder
          .setValue(0.1)))
      .addResources(Resource.newBuilder
        .setName("mem")
        .setRole("*")
        .setType(Value.Type.SCALAR)
        .setScalar(Value.Scalar.newBuilder
          .setValue(24)))
      .build
    val launched:Future[TaskStatus] = mesosClientActor.ask(SubmitTask(task))(taskLaunchTimeout).mapTo[TaskStatus]

    launched.map(taskStatus => {
      log.info(s"launched task with state ${taskStatus.getState}")

      //schedule delete in 10 seconds
      system.scheduler.scheduleOnce(Duration.create(10, TimeUnit.SECONDS),
        () => {
          mesosClientActor.ask(DeleteTask(taskId))(taskDeleteTimeout).mapTo[TaskStatus].map(taskStatus => {
            log.info(s"task killed ended with state ${taskStatus.getState}")
          })
        }
      )


    })



    //handle shutdown
    CoordinatedShutdown(system).addJvmShutdownHook {
      println("custom JVM shutdown hook...")
      val complete:Future[Any] = mesosClientActor.ask(Teardown)(teardownTimeout)
      val result = Await.result(complete, 10.seconds)
      log.info("teardown completed!")
    }

  }

}