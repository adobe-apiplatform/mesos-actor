package mesos

import akka.actor.ActorSystem
import akka.actor.CoordinatedShutdown
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.util.Timeout
import java.time.Instant
import java.util.UUID
import org.apache.mesos.v1.Protos.TaskStatus
import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration._

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
        val mesosClientActor = system.actorOf(MesosClient.props(
            "sample-" + UUID.randomUUID(),
            "sample-framework",
            "http://192.168.99.100:5050",
            "*"
        ))

        //mesosClientActor ! Subscribe

        mesosClientActor.ask(Subscribe)(subscribeTimeout).mapTo[SubscribeComplete].onComplete(complete => {
            log.info("subscribe completed successfully...")
        })

        val taskId = s"sample-task-0-${Instant.now.getEpochSecond}"

        val task = TaskReqs(taskId, "trinitronx/python-simplehttpserver", 0.1, 24, 8080)

        val launched: Future[TaskDetails] = mesosClientActor.ask(SubmitTask(task))(taskLaunchTimeout).mapTo[TaskDetails]

        launched map { taskDetails =>
            //      val taskHost = taskDetails.taskStatus.getContainerStatus.getNetworkInfos(0).getIpAddresses(0)
            val taskHost = taskDetails.hostname
            val taskPort = taskDetails.taskInfo.getResourcesList.asScala.filter(_.getName == "ports").iterator.next().getRanges.getRange(0).getBegin.toInt
            log.info(s"launched task with state ${taskDetails.taskStatus.getState} on host:port ${taskHost}:${taskPort}")

            //schedule delete in 40 seconds
            system.scheduler.scheduleOnce(40.seconds) {
                log.info(s"removing previously created task ${taskId}")
                mesosClientActor.ask(DeleteTask(taskId))(taskDeleteTimeout).mapTo[TaskStatus].map(taskStatus => {
                    log.info(s"task killed ended with state ${taskStatus.getState}")
                })
            }
        } recover {
            case t => log.error(s"task launch failed ${t.getMessage}", t)
        }

        //handle shutdown
        CoordinatedShutdown(system).addJvmShutdownHook {
            println("custom JVM shutdown hook...")
            val complete: Future[Any] = mesosClientActor.ask(Teardown)(teardownTimeout)
            val result = Await.result(complete, 10.seconds)
            log.info("teardown completed!")
        }

    }

}
