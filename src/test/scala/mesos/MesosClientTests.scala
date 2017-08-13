/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package mesos

import akka.actor.ActorSystem
import akka.actor.Props
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes
import akka.pattern.ask
import akka.testkit.ImplicitSender
import akka.testkit.TestKit
import akka.util.Timeout
import org.apache.mesos.v1.Protos.AgentID
import org.apache.mesos.v1.Protos.FrameworkID
import org.apache.mesos.v1.Protos.TaskID
import org.apache.mesos.v1.Protos.TaskState
import org.apache.mesos.v1.Protos.TaskStatus
import org.apache.mesos.v1.scheduler.Protos.Call
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Matchers
import org.scalatest.WordSpecLike
import scala.concurrent.Future
import scala.concurrent.duration._

class MesosClientTests extends TestKit(ActorSystem("MySpec")) with ImplicitSender
        with WordSpecLike with Matchers with BeforeAndAfterAll {
    implicit val ec = system.dispatcher
    override def afterAll {
        TestKit.shutdownActorSystem(system)
    }

    val subscribeCompleteMsg = SubscribeComplete()
    val mesosClient = system.actorOf(Props(new MesosClientActor with MesosClientConnection {override val id: String = "testid"
        override val frameworkName: String = "testframework"
        override val master: String = "none"
        override val role: String = "*"
        override val taskMatcher: TaskMatcher = DefaultTaskMatcher
        override val taskBuilder: TaskBuilder = DefaultTaskBuilder

        override def exec(call: Call): Future[HttpResponse] = {
            log.info(s"sending ${call.getType}")
            call.getType match {
                case Call.Type.ACCEPT =>
                    Future.successful(HttpResponse(StatusCodes.OK)).andThen { case r => sender() ! "ACCEPT_SENT"}
                case _ => Future.failed(new Exception(s"unhandled call type ${call.getType}"))
            }
        }

        override def subscribe(frameworkID: FrameworkID, frameworkName: String): Future[SubscribeComplete] = {
            Future.successful(subscribeCompleteMsg)
        }
    }))

    "An MesosClientActor actor" must {

        "launch submitted tasks to RUNNING (+ healthy) after offers are received" in {

            //subscribe
            mesosClient ! Subscribe
            mesosClient.ask(Subscribe)(Timeout(1.second)).mapTo[SubscribeComplete].onComplete(complete => {
                system.log.info("subscribe completed successfully...")
            })
            expectMsg(subscribeCompleteMsg)

            //submit the task
            mesosClient ! SubmitTask(TaskReqs("taskId1", "fake-docker-image", 0.1, 256, 8080))

            //receive offers
            mesosClient ! ProtobufUtil.getOffers("/offer1.json")

            //verify that ACCEPT was sent
            expectMsg("ACCEPT_SENT")
            //wait for post accept

            val agentId = AgentID.newBuilder()
                    .setValue("db6b062d-84e3-4a2e-a8c5-98ffa944a304-S0")
                    .build()
            //receive the task details after successful launch
            system.log.info("sending UPDATE")

            mesosClient ! org.apache.mesos.v1.scheduler.Protos.Event.Update.newBuilder()
                    .setStatus(TaskStatus.newBuilder()
                            .setTaskId(TaskID.newBuilder().setValue("taskId1"))
                            .setState(TaskState.TASK_STAGING)
                            .setAgentId(agentId)
                            .build())
                    .build()
            //verify that UPDATE was received


            mesosClient ! org.apache.mesos.v1.scheduler.Protos.Event.Update.newBuilder()
                    .setStatus(TaskStatus.newBuilder()
                            .setTaskId(TaskID.newBuilder().setValue("taskId1"))
                            .setState(TaskState.TASK_RUNNING)
                            .setAgentId(agentId)
                            .setHealthy(false)
                            .build())
                    .build()

            //verify that UPDATE was received
            //verify that task is in RUNNING (but NOT healthy) state




            //verify that UPDATE was received
            //verify that task is in RUNNING (AND healthy) state

            mesosClient ! org.apache.mesos.v1.scheduler.Protos.Event.Update.newBuilder()
                    .setStatus(TaskStatus.newBuilder()
                            .setTaskId(TaskID.newBuilder().setValue("taskId1"))
                            .setState(TaskState.TASK_RUNNING)
                            .setAgentId(agentId)
                            .setHealthy(true).build())
                    .build()
            val runningTaskStatus = TaskStatus.newBuilder()
                    .setTaskId(TaskID.newBuilder().setValue("taskId1"))
                    .setState(TaskState.TASK_RUNNING)
                    .setAgentId(agentId)
                    .setHealthy(true).build()
            val runningTaskInfo = ProtobufUtil.getTaskInfo("/taskdetails.json")
            val expectedTaskDetails = Running(runningTaskInfo, runningTaskStatus, "192.168.99.100")

            expectMsg(expectedTaskDetails)

        }

    }



}