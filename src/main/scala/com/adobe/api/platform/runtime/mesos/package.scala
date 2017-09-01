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

package com.adobe.api.platform.runtime

import akka.event.LoggingAdapter
import akka.http.scaladsl.model.ContentType
import akka.http.scaladsl.model.MediaType
import akka.http.scaladsl.model.MediaType.Compressible
import com.google.protobuf.util.JsonFormat
import org.apache.mesos.v1.Protos
import org.apache.mesos.v1.Protos.CommandInfo
import org.apache.mesos.v1.Protos.ContainerInfo
import org.apache.mesos.v1.Protos.ContainerInfo.DockerInfo
import org.apache.mesos.v1.Protos.ContainerInfo.DockerInfo.PortMapping
import org.apache.mesos.v1.Protos.HealthCheck
import org.apache.mesos.v1.Protos.HealthCheck.TCPCheckInfo
import org.apache.mesos.v1.Protos.Offer
import org.apache.mesos.v1.Protos.OfferID
import org.apache.mesos.v1.Protos.Resource
import org.apache.mesos.v1.Protos.TaskID
import org.apache.mesos.v1.Protos.TaskInfo
import org.apache.mesos.v1.Protos.Value
import org.apache.mesos.v1.Protos.Value.Ranges
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer


package object mesos {
    object DefaultTaskMatcher extends TaskMatcher {
        override def matchTasksToOffers(role: String, t: Iterable[TaskReqs], o: Iterable[Offer],
            builder: TaskBuilder)(implicit logger:LoggingAdapter): Map[OfferID, Seq[(TaskInfo, Seq[Int])]] = {
            //we can launch many tasks on a single offer

            var tasksInNeed: ListBuffer[TaskReqs] = t.to[ListBuffer]
            var result = Map[OfferID, Seq[(TaskInfo, Seq[Int])]]()
            var acceptedOfferAgent: String = null //accepted offers must reside on single agent: https://github.com/apache/mesos/blob/master/src/master/validation.cpp#L1768
            o.map(offer => {

                //TODO: manage explicit and default roles, similar to https://github.com/mesos/kafka/pull/103/files
                val portsItr = offer.getResourcesList.asScala
                        .filter(res => res.getName == "ports").iterator
                val hasSomePorts = !portsItr.isEmpty && portsItr.next().getRanges.getRangeList.size() > 0
                if (!hasSomePorts) {
                    //TODO: log info about skipping due to lack of ports...
                    logger.warning("no ports!!!")
                }

                val agentId = offer.getAgentId.getValue
                if (hasSomePorts && (acceptedOfferAgent == null || acceptedOfferAgent == agentId)) {
                    acceptedOfferAgent = agentId
                    val resourcesRaw = offer.getResourcesList.asScala
                            .filter(_.getRole == role) //ignore resources with other roles
                            .filter(res => Seq("cpus", "mem", "ports").contains(res.getName))
                            .groupBy(_.getName)
                    if (resourcesRaw.size == 3) {
                        val resources = resourcesRaw.mapValues(resources => {
                            resources.iterator.next().getScalar.getValue
                        })



                        var remainingOfferCpus = resources("cpus")
                        var remainingOfferMem = resources("mem")
                        var usedPorts = ListBuffer[Int]()
                        var acceptedTasks = ListBuffer[(TaskInfo, Seq[Int])]()
                        tasksInNeed.map(task => {

                            val taskCpus = task.cpus
                            val taskMem = task.mem

                            //collect ranges from ports resources
                            val offerPortsRanges = offer.getResourcesList.asScala
                                                                    .filter(res => res.getName == "ports").map(res => res.getRanges)
                            //plunk the number of ports needed for this task
                            val hostPorts = pluckPorts(offerPortsRanges, task.ports.size, usedPorts)

                            //check for a good fit
                            if (remainingOfferCpus > taskCpus &&
                                    remainingOfferMem > taskMem &&
                                    hostPorts.size == task.ports.size) {
                                //mark resources as used
                                remainingOfferCpus -= taskCpus
                                remainingOfferMem -= taskMem
                                usedPorts ++= hostPorts

                                //build port mappings
                                val portMappings = if (task.ports.isEmpty) List() else for (i <- task.ports.indices) yield PortMapping.newBuilder
                                        .setContainerPort(task.ports(i))
                                        .setHostPort(hostPorts(i))
                                        .build()

                                //build resources
                                val taskResources = ListBuffer[Resource]()

                                task.ports.indices.foreach(i => {
                                    taskResources += Resource.newBuilder()
                                            .setName("ports")
                                            .setType(Value.Type.RANGES)
                                            .setRanges(Ranges.newBuilder()
                                                    .addRange(Value.Range.newBuilder()
                                                            .setBegin(hostPorts(i))
                                                            .setEnd(hostPorts(i))))
                                            .build()
                                })

                                taskResources += Resource.newBuilder()
                                    .setName("cpus")
                                    .setRole("*")
                                    .setType(Value.Type.SCALAR)
                                    .setScalar(Value.Scalar.newBuilder
                                        .setValue(taskCpus))
                                    .build()

                                taskResources += Resource.newBuilder
                                    .setName("mem")
                                    .setRole("*")
                                    .setType(Value.Type.SCALAR)
                                    .setScalar(Value.Scalar.newBuilder
                                            .setValue(taskMem))
                                    .build()

                                //move the task from InNeed to Accepted
                                acceptedTasks += (builder(task, offer, taskResources, portMappings) -> hostPorts)
                                tasksInNeed -= task
                            }
                        })
                        if (!acceptedTasks.isEmpty) {
                            result += (offer.getId -> acceptedTasks)
                        }


                    }

                } else {
                    //log.info("ignoring offers for other slaves for now")
                }
                result
            })
            result
        }
    }

    object DefaultTaskBuilder extends TaskBuilder {

        def apply(reqs: TaskReqs, offer: Offer, resources: Seq[Resource], portMappings: Seq[PortMapping])(implicit logger:LoggingAdapter): TaskInfo = {
            val healthCheck = reqs.healthCheckPortIndex.map(i =>  HealthCheck.newBuilder()
                    .setType(HealthCheck.Type.TCP)
                    .setTcp(TCPCheckInfo.newBuilder()
                            .setPort(reqs.ports(i) ))
                    .setDelaySeconds(0)
                    .setIntervalSeconds(1)
                    .setTimeoutSeconds(1)
                    .setGracePeriodSeconds(25)
                    .build())

            val environmentVars = reqs.environment.map(e => Protos.Environment.Variable.newBuilder
                    .setName(e._1)
                    .setValue(e._2)
                    .build()).asJava


            val taskBuilder = TaskInfo.newBuilder
                    .setName(reqs.taskId)
                    .setTaskId(TaskID.newBuilder
                            .setValue(reqs.taskId))
                    .setAgentId(offer.getAgentId)
                    .setCommand(CommandInfo.newBuilder
                            .setEnvironment(Protos.Environment.newBuilder
                                            .addAllVariables(environmentVars)
                                            .build())
                            .setShell(false)
                            .build())
                    .setContainer(ContainerInfo.newBuilder
                            .setType(ContainerInfo.Type.DOCKER)
                            .setDocker(DockerInfo.newBuilder
                                    .setImage(reqs.dockerImage)
                                    .setNetwork(DockerInfo.Network.BRIDGE)
                                    .addAllPortMappings(portMappings.asJava)
                                    .build())

                            .build())
                    .addAllResources(resources.asJava)
            healthCheck match {
                case Some(hc) => taskBuilder.setHealthCheck(hc)
                case None => //no health check
            }
            taskBuilder.build()
        }
    }

    def toCompactJsonString(message: com.google.protobuf.Message) =
        JsonFormat.printer.omittingInsignificantWhitespace.print(message)

    val protobufContentType = ContentType(MediaType.applicationBinary("x-protobuf", Compressible, "proto"))

    def pluckPorts (rangesList: Iterable[org.apache.mesos.v1.Protos.Value.Ranges], numberOfPorts: Int, ignorePorts: Seq[Int]) = {
        val ports = ListBuffer[Int]()
        rangesList.foreach(ranges => {
            ranges.getRangeList.asScala.foreach(r => {
                val end = r.getEnd
                var next = r.getBegin
                while (ports.size < numberOfPorts && next <= end) {
                    if (!ignorePorts.contains(next.toInt)) {
                        ports += next.toInt
                    }
                    next +=1
                }
            }

            )
        })
        ports.toList
    }
}
