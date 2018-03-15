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

package com.adobe.api.platform.runtime.mesos

import org.apache.mesos.v1.Protos
import org.apache.mesos.v1.Protos.CommandInfo
import scala.collection.JavaConverters._

trait CommandBuilder {
  def apply(command: CommandDef): CommandInfo
}
class DefaultCommandBuilder extends CommandBuilder {
  override def apply(command: CommandDef): CommandInfo = {
    CommandInfo
      .newBuilder()
      .setEnvironment(
        Protos.Environment
          .newBuilder()
          .addAllVariables(command.environment.map {
            case (key, value) =>
              Protos.Environment.Variable.newBuilder
                .setName(key)
                .setValue(value)
                .build()
          }.asJava))
      .addAllUris(command.uris.map { u =>
        Protos.CommandInfo.URI
          .newBuilder()
          .setCache(u.cache)
          .setExecutable(u.executable)
          .setExtract(u.extract)
          .setValue(u.uri.toString)
          .build()
      }.asJava)
      .setShell(false)
      .build()
  }
}
