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

package com.adobe.api.platform.runtime.mesos.mesos

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes
import akka.pattern.ask
import akka.testkit.ImplicitSender
import akka.testkit.TestKit
import akka.util.Timeout
import com.adobe.api.platform.runtime.mesos._
import org.apache.mesos.v1.Protos.AgentID
import org.apache.mesos.v1.Protos.FrameworkID
import org.apache.mesos.v1.Protos.TaskID
import org.apache.mesos.v1.Protos.TaskState
import org.apache.mesos.v1.Protos.TaskStatus
import org.apache.mesos.v1.scheduler.Protos.Call
import org.apache.mesos.v1.scheduler.Protos.Event
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Matchers
import org.scalatest.WordSpecLike
import org.scalatest.concurrent.Eventually
import org.scalatest.junit.JUnitRunner
import org.scalatest.time.Seconds
import org.scalatest.time.Span
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration._

@RunWith(classOf[JUnitRunner])
class MesosClientTests
    extends TestKit(ActorSystem("MySpec"))
    with ImplicitSender
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with Eventually {
  implicit val ec = system.dispatcher
  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  val subscribeCompleteMsg = SubscribeComplete("someid")

  val taskStore = new LocalTaskStore
  val mesosClient: ActorRef = system.actorOf(
    Props(
      new MesosClientActor(
        () => "testid",
        "testframework",
        0.seconds,
        "none",
        "*",
        1.0,
        new DefaultTaskMatcher,
        new DefaultTaskBuilder,
        taskStore,
        false,
        new Timeout(1.seconds),
        Some(new MesosClientConnection {
          def exec(call: Call): Future[HttpResponse] = {
            system.log.info(s"sending test ${call.getType}")
            call.getType match {
              case Call.Type.ACCEPT =>
                Future.successful(HttpResponse(StatusCodes.OK))
              case _ => Future.failed(new Exception(s"unhandled call type ${call.getType}"))
            }
          }
          def subscribe(frameworkID: FrameworkID,
                        frameworkName: String,
                        role: String,
                        failoverTimeout: FiniteDuration,
                        eventHandler: Event => Unit): Future[SubscribeComplete] = {
            Future.successful(subscribeCompleteMsg)
          }
        }))))

  "An MesosClientActor actor" must {
    "launch submitted tasks to RUNNING (+ healthy) after offers are received" in {

      //subscribe
      mesosClient ! Subscribe
      expectMsg(subscribeCompleteMsg)

      //submit the task
      val taskId = "taskId1"
      mesosClient ! SubmitTask(
        TaskDef(
          taskId,
          "taskId1Name",
          "fake-docker-image",
          0.1,
          256,
          List(8080),
          healthCheckParams = Some(HealthCheckConfig(healthCheckPortIndex = 0)),
          commandDef = Some(CommandDef(environment = Map("__OW_API_HOST" -> "192.168.99.100")))))

      //receive offers
      mesosClient ! ProtobufUtil.getOffers("/offer1.json")

      //wait for task to change to submitted status (this assumes we don't receive task status update before task status is changed!
      eventually(timeout(Span(5, Seconds))) {
        taskStore.get(taskId).get should matchPattern { case Submitted(_, _, _, _, _, _) => }
      }

      val agentId = AgentID
        .newBuilder()
        .setValue("db6b062d-84e3-4a2e-a8c5-98ffa944a304-S0")
        .build()

      //receive the task details after successful launch
      mesosClient ! org.apache.mesos.v1.scheduler.Protos.Event.Update
        .newBuilder()
        .setStatus(
          TaskStatus
            .newBuilder()
            .setTaskId(TaskID.newBuilder().setValue("taskId1"))
            .setState(TaskState.TASK_STAGING)
            .setAgentId(agentId)
            .build())
        .build()

      //verify that UPDATE was received
      mesosClient ! org.apache.mesos.v1.scheduler.Protos.Event.Update
        .newBuilder()
        .setStatus(
          TaskStatus
            .newBuilder()
            .setTaskId(TaskID.newBuilder().setValue("taskId1"))
            .setState(TaskState.TASK_RUNNING)
            .setAgentId(agentId)
            .setHealthy(false)
            .build())
        .build()

      //verify that UPDATE was received
      mesosClient ! org.apache.mesos.v1.scheduler.Protos.Event.Update
        .newBuilder()
        .setStatus(
          TaskStatus
            .newBuilder()
            .setTaskId(TaskID.newBuilder().setValue("taskId1"))
            .setState(TaskState.TASK_RUNNING)
            .setAgentId(agentId)
            .setHealthy(true)
            .build())
        .build()
      val runningTaskStatus = TaskStatus
        .newBuilder()
        .setTaskId(TaskID.newBuilder().setValue("taskId1"))
        .setState(TaskState.TASK_RUNNING)
        .setAgentId(agentId)
        .setHealthy(true)
        .build()
      //verify that UPDATE was received leaving task in RUNNING state
      val expectedTaskDetails = Running("taskId1", agentId.getValue, runningTaskStatus, "192.168.99.100", List(11001))
      expectMsg(expectedTaskDetails)
    }
  }
}
