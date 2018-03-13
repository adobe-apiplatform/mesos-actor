package com.adobe.api.platform.runtime.mesos

import org.apache.mesos.v1.Protos.CommandInfo

trait CommandBuilder {
  def apply(command:CommandDef):CommandInfo
}
