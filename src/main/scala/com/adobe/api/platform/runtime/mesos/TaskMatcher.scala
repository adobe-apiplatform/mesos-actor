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
import org.apache.mesos.v1.Protos.ContainerInfo.DockerInfo.PortMapping
import org.apache.mesos.v1.Protos.Offer
import org.apache.mesos.v1.Protos.OfferID
import org.apache.mesos.v1.Protos.Resource
import org.apache.mesos.v1.Protos.TaskInfo
import org.apache.mesos.v1.Protos.Value
import org.apache.mesos.v1.Protos.Value.Ranges
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

trait TaskMatcher {
  def matchTasksToOffers(role: String, t: Iterable[TaskDef], o: Iterable[Offer], builder: TaskBuilder)(
    implicit logger: LoggingAdapter): Map[OfferID, Seq[(TaskInfo, Seq[Int])]]
}
class DefaultTaskMatcher(isValid: Offer => Boolean = _ => true) extends TaskMatcher {
  override def matchTasksToOffers(role: String, t: Iterable[TaskDef], o: Iterable[Offer], builder: TaskBuilder)(
    implicit logger: LoggingAdapter): Map[OfferID, Seq[(TaskInfo, Seq[Int])]] = {
    //we can launch many tasks on a single offer

    var tasksInNeed: ListBuffer[TaskDef] = t.to[ListBuffer]
    var result = Map[OfferID, Seq[(TaskInfo, Seq[Int])]]()
    var acceptedOfferAgent
      : String = null //accepted offers must reside on single agent: https://github.com/apache/mesos/blob/master/src/master/validation.cpp#L1768
    val sortedOffers = o.toSeq.sortBy(
      _.getResourcesList.asScala
        .find(_.getName == "cpus")
        .map(_.getScalar.getValue))

    sortedOffers.map(offer => {
      try {
        //for testing worst case scenario...
        if (!isValid(offer)) {
          throw new IllegalArgumentException("invalid offer")
        }
        //TODO: manage explicit and default roles, similar to https://github.com/mesos/kafka/pull/103/files
        val portsItr = offer.getResourcesList.asScala
          .filter(_.getRole == role) //ignore resources with other roles
          .filter(res => res.getName == "ports")
          .iterator
        val hasSomePorts = portsItr.nonEmpty && portsItr.next().getRanges.getRangeList.size() > 0

        val agentId = offer.getAgentId.getValue
        if (hasSomePorts && (acceptedOfferAgent == null || acceptedOfferAgent == agentId)) {
          val scalarResources = offer.getResourcesList.asScala
            .filter(_.getRole == role) //ignore resources with other roles
            .filter(res => Seq("cpus", "mem", "ports").contains(res.getName))
            .groupBy(_.getName)
            .mapValues(resources => {
              resources.iterator.next().getScalar.getValue
            })

          if (scalarResources.size == 3) { //map will contain ports even though they are not scalar values
            var remainingOfferCpus = scalarResources("cpus")
            var remainingOfferMem = scalarResources("mem")
            var usedPorts = ListBuffer[Int]()
            var acceptedTasks = ListBuffer[(TaskInfo, Seq[Int])]()
            tasksInNeed.map(task => {

              val taskCpus = task.cpus
              val taskMem = task.mem

              //match constraints
              val constraintChecks = task.constraints.map(c => {
                val offerValue = offer.getAttributesList.asScala.find(_.getName == c.attribute).map(_.getText.getValue)

                offerValue match {
                  case Some(v) =>
                    val matches = v.matches(c.value)
                    (if (c.operator == LIKE) matches else !matches, s"${c.attribute} ${c.operator} ${c.value}")
                  //reg matches v
                  case _ => (false, s"${c.attribute} missing")
                }
              })
              logger.debug(s"constraintChecks ${constraintChecks}")
              //check for a good fit
              //collect ranges from ports resources
              val offerPortsRanges = offer.getResourcesList.asScala
                .filter(res => res.getName == "ports")
                .filter(_.getRole == role) //ignore resources with other roles
                .map(res => res.getRanges)
              //pluck the number of ports needed for this task
              val hostPorts = pluckPorts(offerPortsRanges, task.ports.size, usedPorts)
              val matchedResources = remainingOfferCpus > taskCpus &&
                remainingOfferMem > taskMem &&
                hostPorts.size == task.ports.size
              val matchedConstraints = !constraintChecks.exists(_._1 == false)

              if (!matchedConstraints) {
                logger.debug(s"offer did not match constraints ${task.constraints} (${offer.getAttributesList}) ")
              } else if (!matchedResources) {
                logger.info(
                  s"offer did not match resource requirements cpu:${taskCpus} (${remainingOfferCpus}), mem: ${taskMem}  (${remainingOfferMem}), ports: ${task.ports.size} (${hostPorts.size})")
              } else {
                acceptedOfferAgent = agentId

                //mark resources as used
                remainingOfferCpus -= taskCpus
                remainingOfferMem -= taskMem
                usedPorts ++= hostPorts

                //build port mappings
                val portMappings =
                  if (task.ports.isEmpty) List()
                  else
                    for (i <- task.ports.indices)
                      yield
                        PortMapping.newBuilder
                          .setContainerPort(task.ports(i))
                          .setHostPort(hostPorts(i))
                          .build()

                //build resources
                val taskResources = ListBuffer[Resource]()

                task.ports.indices.foreach(i => {
                  taskResources += Resource
                    .newBuilder()
                    .setName("ports")
                    .setRole(role)
                    .setType(Value.Type.RANGES)
                    .setRanges(
                      Ranges
                        .newBuilder()
                        .addRange(
                          Value.Range
                            .newBuilder()
                            .setBegin(hostPorts(i))
                            .setEnd(hostPorts(i))))
                    .build()
                })

                taskResources += Resource
                  .newBuilder()
                  .setName("cpus")
                  .setRole(role)
                  .setType(Value.Type.SCALAR)
                  .setScalar(Value.Scalar.newBuilder
                    .setValue(taskCpus))
                  .build()

                taskResources += Resource.newBuilder
                  .setName("mem")
                  .setRole(role)
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

          } else {
            logger.debug("missing either cpus, mem, or ports resources")
          }

        } else {
          logger.debug("ignoring offers for other slaves for now")
        }

      } catch {
        case t: Exception => logger.error(s"task matching failed, ignoring offer ${offer.getId} ${t}")
      }
    })
    result
  }

  def pluckPorts(rangesList: Iterable[org.apache.mesos.v1.Protos.Value.Ranges],
                 numberOfPorts: Int,
                 ignorePorts: Seq[Int]) = {
    val ports = ListBuffer[Int]()
    rangesList.foreach(ranges => {
      ranges.getRangeList.asScala.foreach(r => {
        val end = r.getEnd
        var next = r.getBegin
        while (ports.size < numberOfPorts && next <= end) {
          if (!ignorePorts.contains(next.toInt)) {
            ports += next.toInt
          }
          next += 1
        }
      })
    })
    ports.toList
  }
}
