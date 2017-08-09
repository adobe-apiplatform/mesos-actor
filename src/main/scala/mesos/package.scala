import akka.event.LoggingAdapter
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
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

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
package object mesos {
    object DefaultTaskMatcher extends TaskMatcher {
        override def matchTasksToOffers(role: String, t: Iterable[TaskReqs], o: Iterable[Offer],
            builder: TaskBuilder)(implicit logger:LoggingAdapter): Map[OfferID, Seq[TaskInfo]] = {
            //we can launch many tasks on a single offer

            var tasksInNeed: ListBuffer[TaskReqs] = t.to[ListBuffer]
            var result = Map[OfferID, Seq[TaskInfo]]()
            var portIndex = 0
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
                    val resources = offer.getResourcesList.asScala
                            .filter(_.getRole == role) //ignore resources with other roles
                            .filter(res => Seq("cpus", "mem").contains(res.getName))
                            .groupBy(_.getName)
                            .mapValues(resources => {
                                resources.iterator.next().getScalar.getValue
                            })
                    if (resources.size == 2) {
                        var remainingOfferCpus = resources("cpus")
                        var remainingOfferMem = resources("mem")
                        var acceptedTasks = ListBuffer[TaskInfo]()
                        tasksInNeed.map(task => {

                            val taskCpus = task.cpus
                            val taskMem = task.mem

                            //check for a good fit
                            if (remainingOfferCpus > taskCpus &&
                                    remainingOfferMem > taskMem) {
                                remainingOfferCpus -= taskCpus
                                remainingOfferMem -= taskMem
                                //move the task from InNeed to Accepted

                                acceptedTasks += builder(task, offer, portIndex)
                                portIndex += 1
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

        def apply(reqs: TaskReqs, offer: Offer, portIndex: Int)(implicit logger:LoggingAdapter): TaskInfo = {
            val containerPort = reqs.port
            //getting the port from the ranges is hard...
            var hostPort = 0
            var portSeekIndex = 0
            val ranges = offer.getResourcesList.asScala
                    .filter(res => res.getName == "ports").iterator.next().getRanges.getRangeList.asScala
            require(ranges.size > 0, s"no available ports in resources for offer ${offer}")
            val rangesIt = ranges.iterator
            var rangeSeek = rangesIt.next()
            var nextPort = rangeSeek.getBegin
            while (portSeekIndex < portIndex) {
                while (portSeekIndex < portIndex && nextPort < rangeSeek.getEnd) {
                    portSeekIndex += 1
                    nextPort += 1
                }
                if (portSeekIndex != portIndex) {
                    rangeSeek = rangesIt.next()
                    nextPort = rangeSeek.getBegin
                }
            }
            if (portSeekIndex != portIndex) {
                throw new RuntimeException("not enough ports matched in offer")
            } else {
                hostPort = nextPort.toInt
            }

            val agentHost = offer.getHostname
            val dockerImage = reqs.dockerImage

            val healthCheck = HealthCheck.newBuilder()
                    .setType(HealthCheck.Type.TCP)
                    .setTcp(TCPCheckInfo.newBuilder()
                            .setPort(containerPort))
                    .setDelaySeconds(0)
                    .setIntervalSeconds(1)
                    .setTimeoutSeconds(1)
                    .setGracePeriodSeconds(25)

            val task = TaskInfo.newBuilder
                    .setName(reqs.taskId)
                    .setTaskId(TaskID.newBuilder
                            .setValue(reqs.taskId))
                    .setAgentId(offer.getAgentId)
                    .setCommand(CommandInfo.newBuilder
                            .setEnvironment(Protos.Environment.newBuilder
                                    .addVariables(Protos.Environment.Variable.newBuilder
                                            .setName("__OW_API_HOST")
                                            .setValue(agentHost)))
                            .setShell(false)
                            .build())
                    .setContainer(ContainerInfo.newBuilder
                            .setType(ContainerInfo.Type.DOCKER)
                            .setDocker(DockerInfo.newBuilder
                                    .setImage(dockerImage)
                                    .setNetwork(DockerInfo.Network.BRIDGE)
                                    .addPortMappings(PortMapping.newBuilder
                                            .setContainerPort(containerPort)
                                            .setHostPort(hostPort)
                                            .build)
                            ).build())
                    .setHealthCheck(healthCheck)
                    .addResources(Resource.newBuilder()
                            .setName("ports")
                            .setType(Value.Type.RANGES)
                            .setRanges(Ranges.newBuilder()
                                    .addRange(Value.Range.newBuilder()
                                            .setBegin(hostPort)
                                            .setEnd(hostPort))))
                    .addResources(Resource.newBuilder
                            .setName("cpus")
                            .setRole("*")
                            .setType(Value.Type.SCALAR)
                            .setScalar(Value.Scalar.newBuilder
                                    .setValue(reqs.cpus)))
                    .addResources(Resource.newBuilder
                            .setName("mem")
                            .setRole("*")
                            .setType(Value.Type.SCALAR)
                            .setScalar(Value.Scalar.newBuilder
                                    .setValue(reqs.mem)))
                    .build
            task
        }
    }
}
