package com.adobe.api.platform.runtime.mesos.sample

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.cluster.{Cluster, ClusterEvent}
import akka.cluster.ClusterEvent._

object SampleHAFramework {
  def main(args: Array[String]): Unit = {
    val marathonConfig = MarathonConfig.discoverAkkaConfig()
    val clusterName: String = marathonConfig.getString("akka.cluster.name")

    // Create an Akka system
    val system = ActorSystem(clusterName, marathonConfig)

    // Create an actor that handles cluster domain events
    system.actorOf(Props[SimpleClusterListener], name = "clusterListener")
  }

}

class SimpleClusterListener extends Actor with ActorLogging {

  val cluster = Cluster(context.system)

  // subscribe to cluster changes, re-subscribe when restart
  override def preStart(): Unit = {
    cluster.subscribe(self,
      classOf[MemberEvent],
      classOf[UnreachableMember],
      classOf[ClusterEvent.LeaderChanged])
  }
  override def postStop(): Unit = cluster.unsubscribe(self)

  def receive = {
    case state: CurrentClusterState =>
      log.info("Current members: {}. Leader: {}", state.members.size, state.getLeader)
    case MemberUp(member) =>
      log.info("Member is Up: {}", member.address)
    case UnreachableMember(member) =>
      log.info("Member detected as unreachable: {}", member)
    case MemberRemoved(member, previousStatus) =>
      log.info("Member is Removed: {} after {}",
        member.address, previousStatus)
    case LeaderChanged(node) =>
      log.info("Leader changed to {}", node)

    case event: MemberEvent =>
      log.info("Member event at {}, status: {}", event.member.address, event.member.status)
    // ignore

  }

}
