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

import akka.actor.ActorSystem
import akka.actor.CoordinatedShutdown
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.adobe.api.platform.runtime.mesos.DeleteTask
import com.adobe.api.platform.runtime.mesos.MesosClient
import com.adobe.api.platform.runtime.mesos.Running
import com.adobe.api.platform.runtime.mesos.SubmitTask
import com.adobe.api.platform.runtime.mesos.Subscribe
import com.adobe.api.platform.runtime.mesos.SubscribeComplete
import com.adobe.api.platform.runtime.mesos.TaskReqs
import com.adobe.api.platform.runtime.mesos.Teardown
import java.time.Instant
import java.util.UUID
import org.apache.mesos.v1.Protos.TaskStatus
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
        implicit val system = ActorSystem("sample-framework-system")
        implicit val mat = ActorMaterializer()
        implicit val log = system.log
        implicit val ec = system.dispatcher

        val taskLaunchTimeout = Timeout(15 seconds)
        val taskDeleteTimeout = Timeout(10 seconds)
        val subscribeTimeout = Timeout(5 seconds)
        val teardownTimeout = Timeout(5 seconds)

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

        //in this sample, container port 8080 listens, and 8081 does NOT listen; so using 8081 for health checks will always fail
        val task = TaskReqs(taskId, "trinitronx/python-simplehttpserver", 0.1, 24, List(8080, 8081), Some(0))

        val launched: Future[Running] = mesosClientActor.ask(SubmitTask(task))(taskLaunchTimeout).mapTo[Running]

        launched map { taskDetails =>
            val taskHost = taskDetails.hostname
            val taskPorts = taskDetails.hostports
            log.info(s"launched task id ${taskDetails.taskInfo.getTaskId.getValue} with state ${taskDetails.taskStatus.getState} on agent ${taskHost} listening on ports ${taskPorts}")

            //schedule delete in 40 seconds
            system.scheduler.scheduleOnce(10.seconds) {
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
