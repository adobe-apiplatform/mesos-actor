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
import org.apache.mesos.v1.Protos.ContainerInfo.DockerInfo.Network
import org.apache.mesos.v1.Protos.ContainerInfo.DockerInfo.PortMapping
import org.apache.mesos.v1.Protos.Resource
import org.apache.mesos.v1.Protos.Value
import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MesosTaskBuilderTests extends FlatSpec with Matchers {
  behavior of "Mesos Default TaskBuilder"
  implicit val actorSystem: ActorSystem = ActorSystem("test-system")
  implicit val logger: LoggingAdapter = actorSystem.log

  it should "set TaskInfo properties from TaskDef" in {
    val offers = ProtobufUtil.getOffers("/offer1.json")
    val resources = Seq(
      Resource
        .newBuilder()
        .setName("cpus")
        .setType(Value.Type.SCALAR)
        .setScalar(Value.Scalar.newBuilder().setValue(0.1))
        .build())
    val portMappings = Seq(
      PortMapping
        .newBuilder()
        .setHostPort(31000)
        .setContainerPort(112233)
        .build())
    val parameters = Map(
      "dns" -> Set("1.2.3.4", "8.8.8.8"),
      "cap-drop" -> Set("NET_RAW", "NET_ADMIN"),
      "ulimit" -> Set("nofile=1024:1024"))
    val environment = Map("VAR1" -> "VAL1", "VAR2" -> "VAL2")
    val taskDef = TaskDef(
      "taskId",
      "taskName",
      "dockerImage:someTag",
      0.1,
      256,
      List(112233),
      healthCheckParams = Some(HealthCheckConfig(0, 1, 2, 5, gracePeriod = 30, maxConsecutiveFailures = 2)),
      true,
      User("usernet"),
      parameters,
      Some(CommandDef(environment = environment)))
    val taskInfo = new DefaultTaskBuilder()(taskDef, offers.getOffers(0), resources, portMappings)

    taskInfo.getTaskId.getValue shouldBe taskDef.taskId
    taskInfo.getName shouldBe taskDef.taskName
    taskInfo.getContainer.getDocker.getImage shouldBe taskDef.dockerImage
    taskInfo.getContainer.getDocker.getForcePullImage shouldBe true
    taskInfo.getResources(0).getName shouldBe "cpus"
    taskInfo.getResources(0).getScalar.getValue shouldBe taskDef.cpus
    taskInfo.getContainer.getNetworkInfos(0).getName shouldBe "usernet"
    taskInfo.getContainer.getDocker.getNetwork shouldBe Network.USER
    taskInfo.getContainer.getDocker.getPortMappings(0).getContainerPort shouldBe taskDef.ports(0)
    taskInfo.getContainer.getDocker.getPortMappings(0).getHostPort shouldBe 31000
    taskInfo.getContainer.getDocker.getParameters(0).getKey shouldBe "dns"
    taskInfo.getContainer.getDocker.getParameters(0).getValue shouldBe "1.2.3.4"
    taskInfo.getContainer.getDocker.getParameters(1).getKey shouldBe "dns"
    taskInfo.getContainer.getDocker.getParameters(1).getValue shouldBe "8.8.8.8"
    taskInfo.getContainer.getDocker.getParameters(2).getKey shouldBe "cap-drop"
    taskInfo.getContainer.getDocker.getParameters(2).getValue shouldBe "NET_RAW"
    taskInfo.getContainer.getDocker.getParameters(3).getKey shouldBe "cap-drop"
    taskInfo.getContainer.getDocker.getParameters(3).getValue shouldBe "NET_ADMIN"
    taskInfo.getContainer.getDocker.getParameters(4).getKey shouldBe "ulimit"
    taskInfo.getContainer.getDocker.getParameters(4).getValue shouldBe "nofile=1024:1024"
    taskInfo.getCommand.getEnvironment.getVariables(0).getName shouldBe "VAR1"
    taskInfo.getCommand.getEnvironment.getVariables(0).getValue shouldBe "VAL1"
    taskInfo.getCommand.getEnvironment.getVariables(1).getName shouldBe "VAR2"
    taskInfo.getCommand.getEnvironment.getVariables(1).getValue shouldBe "VAL2"
    taskInfo.getHealthCheck.getDelaySeconds shouldBe 1
    taskInfo.getHealthCheck.getIntervalSeconds shouldBe 2
    taskInfo.getHealthCheck.getTimeoutSeconds shouldBe 5
    taskInfo.getHealthCheck.getGracePeriodSeconds shouldBe 30
    taskInfo.getHealthCheck.getConsecutiveFailures shouldBe 2
    taskInfo.getHealthCheck.getTcp.getPort shouldBe 112233
  }

}
