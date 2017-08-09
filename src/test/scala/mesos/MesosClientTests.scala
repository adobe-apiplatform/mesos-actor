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
import akka.testkit.ImplicitSender
import akka.testkit.TestKit
import org.apache.mesos.v1.Protos.Offer
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Matchers
import org.scalatest.WordSpecLike

class MesosClientTests extends TestKit(ActorSystem("MySpec")) with ImplicitSender
        with WordSpecLike with Matchers with BeforeAndAfterAll {
    override def afterAll {
        TestKit.shutdownActorSystem(system)
    }

    val mesosClient = system.actorOf(MesosClientActor.props("test-id", "test-name",
            "http://bogus:5050",
    "*"))

    "An MesosClientActor actor" must {

        "submit tasks to mesos after offers are received" in {

//            val offers = ProtobufUtil.getOffers("/offer1.json")
//
//            //submit the task
//            mesosClient ! SubmitTask(TaskReqs("taskId1", "fake-docker-image", 0.1, 256, 8080))
//
//            //submit offers
//            mesosClient ! offers
//
//
//            expectMsg(TaskDetails)
        }

    }



}
