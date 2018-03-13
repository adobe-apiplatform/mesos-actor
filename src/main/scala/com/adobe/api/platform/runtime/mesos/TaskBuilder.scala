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
import org.apache.mesos.v1.Protos.Resource
import org.apache.mesos.v1.Protos.TaskInfo

trait TaskBuilder {
  def apply(reqs: TaskDef, offer: Offer, resources: Seq[Resource], portMappings: Seq[PortMapping], command:CommandDef = null)(
    implicit logger: LoggingAdapter): TaskInfo
}
