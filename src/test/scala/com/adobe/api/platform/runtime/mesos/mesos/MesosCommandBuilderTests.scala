/*
 * Copyright 2018 Adobe Systems Incorporated. All rights reserved.
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

import java.net.URI

import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import com.adobe.api.platform.runtime.mesos.{CommandDef, CommandURIDef, DefaultCommandBuilder}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class MesosCommandBuilderTests extends FlatSpec with Matchers {
  behavior of "Mesos Default TaskBuilder"
  implicit val actorSystem: ActorSystem = ActorSystem("test-system")
  implicit val logger: LoggingAdapter = actorSystem.log

  it should "set URIs on a Command Proto from CommandDef" in {
    val uris = 0.to(3).map((i: Int) => { new CommandURIDef(new URI(f"http://$i.com")) })
    val command = CommandDef(uris = uris)
    val mesosCommandInfo = new DefaultCommandBuilder()(command)

    mesosCommandInfo.getUris(0).getValue shouldBe "http://0.com"
    mesosCommandInfo.getUris(1).getValue shouldBe "http://1.com"
    mesosCommandInfo.getUris(2).getValue shouldBe "http://2.com"
    mesosCommandInfo.getUris(3).getValue shouldBe "http://3.com"
  }

  it should "retain Options set on CommandDef when creating Command Proto" in {
    val uris = 0
      .to(0)
      .map((i: Int) => {
        new CommandURIDef(new URI(f"http://$i.com"), extract = false, cache = true, executable = true)
      })

    val command = CommandDef(uris = uris)
    val mesosCommandInfo = new DefaultCommandBuilder()(command)

    mesosCommandInfo.getUris(0).getExecutable shouldBe true
    mesosCommandInfo.getUris(0).getCache shouldBe true
    mesosCommandInfo.getUris(0).getExtract shouldBe false

  }

  it should "retain enviormment variables" in {
    val environment = Map("VAR1" -> "VAL1", "VAR2" -> "VAL2")

    val command = CommandDef(environment = environment)
    val mesosCommandInfo = new DefaultCommandBuilder()(command)

    mesosCommandInfo.getEnvironment.getVariables(0).getName shouldBe "VAR1"
    mesosCommandInfo.getEnvironment.getVariables(0).getValue shouldBe "VAL1"
    mesosCommandInfo.getEnvironment.getVariables(1).getName shouldBe "VAR2"
    mesosCommandInfo.getEnvironment.getVariables(1).getValue shouldBe "VAL2"

  }

}
