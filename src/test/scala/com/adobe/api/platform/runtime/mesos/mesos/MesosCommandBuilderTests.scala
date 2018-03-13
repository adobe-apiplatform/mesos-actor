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
  implicit val actorSystem:ActorSystem = ActorSystem("test-system")
  implicit val logger:LoggingAdapter = actorSystem.log

  it should "set URIs on a Command Proto from CommandDef" in {
    val uris = 0.to(3).map((i:Int) => {new CommandURIDef(new URI(f"http://$i.com"))})
    val command = CommandDef(uris = uris)
    val mesosCommandInfo = DefaultCommandBuilder.apply(command)

    mesosCommandInfo.getUris(0).getValue shouldBe "http://0.com"
    mesosCommandInfo.getUris(1).getValue shouldBe "http://1.com"
    mesosCommandInfo.getUris(2).getValue shouldBe "http://2.com"
    mesosCommandInfo.getUris(3).getValue shouldBe "http://3.com"
  }

  it should "retain Options set on CommandDef when creating Command Proto" in {
    val uris = 0.to(0).map((i:Int) => {new CommandURIDef(new URI(f"http://$i.com"),extract = false, cache = true, executable = true)})

    val command = CommandDef(uris = uris)
    val mesosCommandInfo = DefaultCommandBuilder.apply(command)

    mesosCommandInfo.getUris(0).getExecutable shouldBe true
    mesosCommandInfo.getUris(0).getCache shouldBe true
    mesosCommandInfo.getUris(0).getExtract shouldBe false

  }

  it should "retain enviormment variables" in {
    val environment = Map("VAR1" -> "VAL1", "VAR2" -> "VAL2")

    val command = CommandDef(environment = environment)
    val mesosCommandInfo = DefaultCommandBuilder.apply(command)

    mesosCommandInfo.getEnvironment.getVariables(0).getName shouldBe "VAR1"
    mesosCommandInfo.getEnvironment.getVariables(0).getValue shouldBe "VAL1"
    mesosCommandInfo.getEnvironment.getVariables(1).getName shouldBe "VAR2"
    mesosCommandInfo.getEnvironment.getVariables(1).getValue shouldBe "VAL2"

  }


}
