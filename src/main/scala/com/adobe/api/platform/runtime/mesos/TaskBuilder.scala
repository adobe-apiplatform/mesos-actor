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
import org.apache.mesos.v1.Protos
import org.apache.mesos.v1.Protos.CommandInfo
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
  def apply(reqs: TaskDef,
            offer: Offer,
            resources: Seq[Resource],
            portMappings: Seq[PortMapping],
            command: CommandDef = null)(implicit logger: LoggingAdapter): TaskInfo
}

object DefaultTaskBuilder {
  def apply() = new DefaultTaskBuilder
}
class DefaultTaskBuilder extends TaskBuilder {

  def apply(reqs: TaskDef, offer: Offer, resources: Seq[Resource], portMappings: Seq[PortMapping])(
    implicit logger: LoggingAdapter): TaskInfo = {
    val healthCheck = reqs.healthCheckPortIndex.map(
      i =>
        HealthCheck
          .newBuilder()
          .setType(HealthCheck.Type.TCP)
          .setTcp(TCPCheckInfo
            .newBuilder()
            .setPort(reqs.ports(i)))
          .setDelaySeconds(0)
          .setIntervalSeconds(1)
          .setTimeoutSeconds(1)
          .setGracePeriodSeconds(25)
          .build())

    val environmentVars = reqs.environment
      .map(
        e =>
          Protos.Environment.Variable.newBuilder
            .setName(e._1)
            .setValue(e._2)
            .build())
      .asJava

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
      .setCommand(
        CommandInfo.newBuilder
          .setEnvironment(Protos.Environment.newBuilder
            .addAllVariables(environmentVars)
            .build())
          .setShell(false)
          .build())
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
    healthCheck match {
      case Some(hc) => taskBuilder.setHealthCheck(hc)
      case None     => //no health check
    }
    taskBuilder.build()
  }
}
