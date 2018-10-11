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
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.adobe.api.platform.runtime.mesos._
import java.time.Instant
import java.util.UUID

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration._

/**
 * An example of a Mesos Framework. It will:
 * - start the framework communication with mesos master
 * - launch 3 tasks
 * - kill each task after 10s
 * - tear down the framework after 30s, then exits
 */
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

    val mesosClientActor = system.actorOf(
      MesosClient.props(
        () => "sample-" + UUID.randomUUID(),
        "sample-framework",
        "http://192.168.99.100:5050",
        "sample-role",
        30.seconds,
        taskStore = new LocalTaskStore))

    mesosClientActor
      .ask(Subscribe)(subscribeTimeout)
      .mapTo[SubscribeComplete]
      .onComplete(complete => {
        log.info("subscribe completed successfully...")
      })

    var taskCount = 0
    def nextName() = {
      taskCount += 1
      s"sample-task-${Instant.now.getEpochSecond}-${taskCount}"
    }
    def nextId() = "sample-task-" + UUID.randomUUID()

    (1 to 3).foreach(_ => {
      val task = TaskDef(
        nextId(),
        nextName(),
        "trinitronx/python-simplehttpserver",
        0.1,
        24,
        List(8080, 8081),
        Some(HealthCheckConfig(0)))
      val launched: Future[TaskState] = mesosClientActor.ask(SubmitTask(task))(taskLaunchTimeout).mapTo[TaskState]
      launched map {
        case taskDetails: Running => {
          val taskHost = taskDetails.hostname
          val taskPorts = taskDetails.hostports
          log.info(
            s"launched task id ${taskDetails.taskId} with state ${taskDetails.taskStatus.getState} on agent ${taskHost} listening on ports ${taskPorts}")

          //schedule delete in 10 seconds
          system.scheduler.scheduleOnce(10.seconds) {
            log.info(s"removing previously created task ${taskDetails.taskId}")
            mesosClientActor
              .ask(DeleteTask(taskDetails.taskId))(taskDeleteTimeout)
              .mapTo[Deleted]
              .map(deleted => {
                log.info(s"task killed ended with state ${deleted.taskStatus.getState}")
              })
          }
        }
        case s => log.error(s"failed to launch task; state is ${s}")
      } recover {
        case t => log.error(s"task launch failed ${t.getMessage}", t)
      }
    })

    system.scheduler.scheduleOnce(30.seconds) {
      val complete: Future[Any] = mesosClientActor.ask(Teardown)(teardownTimeout)
      Await.result(complete, 10.seconds)
      println("teardown completed!")
      system.terminate().map(_ => System.exit(0))
    }

  }

}
