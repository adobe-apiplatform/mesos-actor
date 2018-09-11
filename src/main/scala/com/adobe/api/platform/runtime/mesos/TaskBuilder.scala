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

package com.adobe.api.platform.runtime.mesos

import akka.event.LoggingAdapter
import org.apache.mesos.v1.Protos.ContainerInfo
import org.apache.mesos.v1.Protos.ContainerInfo.DockerInfo
import org.apache.mesos.v1.Protos.ContainerInfo.DockerInfo.PortMapping
import org.apache.mesos.v1.Protos.HealthCheck
import org.apache.mesos.v1.Protos.HealthCheck.TCPCheckInfo
import org.apache.mesos.v1.Protos.NetworkInfo
import org.apache.mesos.v1.Protos.Offer
import org.apache.mesos.v1.Protos.Parameter
import org.apache.mesos.v1.Protos.Resource
import org.apache.mesos.v1.Protos.TaskID
import org.apache.mesos.v1.Protos.TaskInfo
import scala.collection.JavaConverters._

trait TaskBuilder {
  def commandBuilder: CommandBuilder
  def apply(reqs: TaskDef, offer: Offer, resources: Seq[Resource], portMappings: Seq[PortMapping])(
    implicit logger: LoggingAdapter): TaskInfo
}

class DefaultTaskBuilder extends TaskBuilder {
  val commandBuilder = new DefaultCommandBuilder()
  def apply(reqs: TaskDef, offer: Offer, resources: Seq[Resource], portMappings: Seq[PortMapping])(
    implicit logger: LoggingAdapter): TaskInfo = {

    val parameters = reqs.dockerRunParameters.flatMap {
      case (k, v) =>
        v.map(pv => Parameter.newBuilder().setKey(k).setValue(pv).build())
    }.asJava

    val dockerNetwork = reqs.network match {
      case _: User => DockerInfo.Network.USER
      case Host    => DockerInfo.Network.HOST
      case Bridge  => DockerInfo.Network.BRIDGE
    }

    //for case of user network, create a single NetworkInfo with the name
    val networkInfos = reqs.network match {
      case u: User =>
        Seq[NetworkInfo](
          NetworkInfo
            .newBuilder()
            .setName(u.name)
            .build()).asJava
      case _ => Seq[NetworkInfo]().asJava
    }
    val taskBuilder = TaskInfo.newBuilder
      .setName(reqs.taskName)
      .setTaskId(TaskID.newBuilder
        .setValue(reqs.taskId))
      .setAgentId(offer.getAgentId)
      .setContainer(
        ContainerInfo.newBuilder
          .setType(ContainerInfo.Type.DOCKER)
          .addAllNetworkInfos(networkInfos)
          .setDocker(
            DockerInfo.newBuilder
              .setImage(reqs.dockerImage)
              .setNetwork(dockerNetwork)
              .addAllParameters(parameters)
              .addAllPortMappings(portMappings.asJava)
              .build())
          .build())
      .addAllResources(resources.asJava)
    reqs.commandDef.foreach(c => {
      taskBuilder.setCommand(commandBuilder(c))
    })
    reqs.healthCheckPortIndex.foreach(h => {
      taskBuilder.setHealthCheck(
        HealthCheck
          .newBuilder()
          .setType(HealthCheck.Type.TCP)
          .setTcp(TCPCheckInfo
            .newBuilder()
            .setPort(reqs.ports(h)))
          .setDelaySeconds(reqs.healthCheckParams.getOrElse("delay", 0).doubleValue())
          .setIntervalSeconds(reqs.healthCheckParams.getOrElse("interval", 1).doubleValue())
          .setTimeoutSeconds(reqs.healthCheckParams.getOrElse("timeout", 1).doubleValue())
          .setGracePeriodSeconds(reqs.healthCheckParams.getOrElse("gracePeriod", 25).doubleValue())
          .setConsecutiveFailures(reqs.healthCheckParams.getOrElse("maxConsecutiveFailures", 3))
          .build())
    })
    taskBuilder.build()
  }
}
