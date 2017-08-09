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
import akka.event.LoggingAdapter
import org.apache.mesos.v1.Protos.Offer
import org.apache.mesos.v1.scheduler.Protos.Event.Offers
import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.junit.JUnitRunner
import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class MesosTaskMatcherTests extends FlatSpec with Matchers {

    implicit val actorSystem:ActorSystem = ActorSystem("test-system")
    implicit val logger:LoggingAdapter = actorSystem.log

    behavior of "Mesos Default TaskMatcher"

    it should "only use a single slave per accept" in {
        val offers = ProtobufUtil.getOffers("/offer1.json")

        val tasks = List[TaskReqs](TaskReqs("taskId", "dockerImage:someTag", 0.1, 256, 8080))
        val taskMap = DefaultTaskMatcher.matchTasksToOffers("*", tasks, offers.getOffersList.asScala, DefaultTaskBuilder)

        taskMap.size shouldBe 1

    }
    it should "not use an offer if the number of required ports are not available" in {
        val offers = ProtobufUtil.getOffers("/offer-noports.json")

        val tasks = List[TaskReqs](TaskReqs("taskId", "dockerImage:someTag", 0.1, 256, 8080))
        val taskMap = DefaultTaskMatcher.matchTasksToOffers("*", tasks, offers.getOffersList.asScala, DefaultTaskBuilder)

        taskMap.size shouldBe 0
    }

}
