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

package com.adobe.api.platform.runtime.mesos.sample

import akka.actor.Address
import java.io.File
import java.net.URL
import com.typesafe.config._
import collection.JavaConverters._

/**
 * This object discovers seed nodes for the Akka Cluster using Marathon API
 */
object MarathonConfig {

  /**
   * Use Marathon API to discover other running tasks for this app.
   *
   * A task  may come as:
   *
   * {
          id: "akka-cluster.086db21b-7192-11e7-8203-0242ac107905",
          slaveId: "35f9af86-f5b0-4e95-a2b1-5f201b10fbaa-S0",
          host: "localhost",
          state: "TASK_RUNNING",
          startedAt: "2017-07-25T23:36:09.629Z",
          stagedAt: "2017-07-25T23:36:07.949Z",
          ports: [
            11696
          ],
          version: "2017-07-25T23:36:07.294Z",
          ipAddresses: [
            {
              ipAddress: "172.17.0.2",
              protocol: "IPv4"
            }
          ],
          appId: "/akka-cluster"
      }
   * This method extracts the host and the port of each task
   *
   * @return an array of strings with akka.tcp://{cluster-name}@{IP}:{PORT}
   */
  def getSeedNodes(config: Config): Seq[Address] = {
    val url: String = config.getString("akka.cluster.discovery.url")
    val portIndex: Int = config.getInt("akka.cluster.discovery.port-index")
    val clusterName: String = config.getString("akka.cluster.name")

    var tmpCfg: Config = null

    if (url.startsWith("http")) {
      tmpCfg = ConfigFactory
        .parseURL(new URL(url), ConfigParseOptions.defaults().setSyntax(ConfigSyntax.JSON))
        .resolve()
    } else {
      tmpCfg = ConfigFactory
        .parseFileAnySyntax(new File(url))
        .resolve()
    }
    var notHealthyTasks = unhealthyTasks(tmpCfg)

    while (notHealthyTasks.size != 0) {
      val unhealthyTaskIds = notHealthyTasks.map(t => t.getString("id"))
      System.out.println(s"found ${notHealthyTasks.size} unhealthy tasks (${unhealthyTaskIds}), will try again in 5s")
      System.out.println()
      Thread.sleep(5000)
      if (url.startsWith("http")) {
        tmpCfg = ConfigFactory
          .parseURL(new URL(url), ConfigParseOptions.defaults().setSyntax(ConfigSyntax.JSON))
          .resolve()
      } else {
        tmpCfg = ConfigFactory
          .parseFileAnySyntax(new File(url))
          .resolve()
      }
      notHealthyTasks = unhealthyTasks(tmpCfg)
    }

    System.out.println(s"can start cluster now that all ${tmpCfg.getConfigList("tasks").size()} tasks are healthy")

    var seq: Seq[Address] = Seq()
    tmpCfg
      .getConfigList("tasks")
      .asScala
      .foreach((item: Config) =>
        seq = seq :+ Address("akka.tcp", clusterName, item.getString("host"), item.getIntList("ports").get(portIndex)))
    //for testing, case the first task to commit suicide
    //    if (tmpCfg.getConfigList("tasks").get(0).getString("id") == System.getenv("MESOS_TASK_ID")){
    //        System.out.println("THIS IS THE FIRST MARATHON TASK, COMMITTING SUICIDE")
    //        Thread.sleep(5000)
    //        System.exit(129)
    //    }

    seq
  }
  private def unhealthyTasks(tmpCfg: Config) =
    tmpCfg
      .getConfigList("tasks")
      .asScala
      .filter(
        t =>
          t.hasPath("healthCheckResults") == false || t
            .getConfigList("healthCheckResults")
            .get(0)
            .getBoolean("alive") == false)

}
