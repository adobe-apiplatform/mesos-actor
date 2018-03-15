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

import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import com.adobe.api.platform.runtime.mesos._
import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.junit.JUnitRunner
import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class MesosTaskMatcherTests extends FlatSpec with Matchers {

  implicit val actorSystem: ActorSystem = ActorSystem("test-system")
  implicit val logger: LoggingAdapter = actorSystem.log

  behavior of "Mesos Default TaskMatcher"

  it should "only use a single slave per accept" in {
    val offers = ProtobufUtil.getOffers("/offer1.json")

    val tasks = List[TaskDef](TaskDef("taskId", "taskName", "dockerImage:someTag", 0.1, 256, List(8080)))
    val taskMap =
      new DefaultTaskMatcher().matchTasksToOffers("*", tasks, offers.getOffersList.asScala, new DefaultTaskBuilder())

    taskMap.size shouldBe 1

  }
  it should "not use an offer if the number of required ports are not available" in {
    val offers = ProtobufUtil.getOffers("/offer-noports.json")

    val tasks = List[TaskDef](TaskDef("taskId", "taskName", "dockerImage:someTag", 0.1, 256, List(8080)))
    val taskMap =
      new DefaultTaskMatcher().matchTasksToOffers("*", tasks, offers.getOffersList.asScala, new DefaultTaskBuilder())

    taskMap.size shouldBe 0
  }
  it should "only match an offer if contstraints match" in {
    val offers = ProtobufUtil.getOffers("/offer1.json")

    val tasks = List[TaskDef](
      TaskDef(
        "taskId",
        "taskName",
        "dockerImage:someTag",
        0.1,
        256,
        List(8080),
        constraints = Set(Constraint("a1", LIKE, "av1"), Constraint("a2", UNLIKE, "av2"))), //test missing attributes
      TaskDef(
        "taskId2",
        "taskName",
        "dockerImage:someTag",
        0.1,
        256,
        List(8080),
        constraints = Set(Constraint("att1", LIKE, "att1value.*"), Constraint("att2", UNLIKE, "notmatched"))), //test matching multiple constraints
      TaskDef(
        "taskId3",
        "taskName",
        "dockerImage:someTag",
        0.1,
        256,
        List(8080),
        constraints = Set(Constraint("att1", LIKE, "att1valueslave1"))), //test exact match
      TaskDef(
        "taskId4",
        "taskName",
        "dockerImage:someTag",
        0.1,
        256,
        List(8080),
        constraints = Set(Constraint("att2", UNLIKE, "att1value.*"))), //test UNLIKE matching the opposite
      TaskDef(
        "taskId5",
        "taskName",
        "dockerImage:someTag",
        0.1,
        256,
        List(8080),
        constraints = Set(Constraint("att2", LIKE, "(?!att1value).*")))) //test negative lookahead to match same as UNLIKE
    val taskMap =
      new DefaultTaskMatcher().matchTasksToOffers("*", tasks, offers.getOffersList.asScala, new DefaultTaskBuilder())
    taskMap.values.flatten.map(_._1.getTaskId.getValue) shouldBe List("taskId2", "taskId3", "taskId4", "taskId5")

  }
}
