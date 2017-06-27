# mesos-client-scala

A lightweight Mesos meta-framework for assembling Mesos Frameworks (schedulers). 
Based on:
* Akka
* Akka Streams
* Akka HTTP
* Mesos scheduler HTTP API [http://mesos.apache.org/documentation/latest/scheduler-http-api/]


# Example

See `src/main/scala/mesos/SampleFramework.scala` for an example framework that:
* initiates the Mesos Client subscription
* submits a task for execution
* kills the task after some time
* shuts down the Mesos Client on application termination 

# Usage

To use in your own application:
* copy `src/main/scala/mesos/MesosClient.scala` to your app (TODO: package as a library...)
* add dependencies to your app
```text
    //for mesos protobuf
    compile "org.apache.mesos:mesos:1.0.0"
    //for recordIO
    compile "com.lightbend.akka:akka-stream-alpakka-simple-codecs_2.11:0.9"
    //for JSON printing of protobuf
    compile "com.google.protobuf:protobuf-java-util:3.3.1"
```

* setup task builder function
(You can also customize mapping between Offers and Tasks using taskMatcher param in MesosClientActor.props)
```text
  def buildTask(reqs:TaskReqs, offer:Offer):TaskInfo = {
    val containerPort = reqs.port
    val hostPort = offer.getResourcesList.asScala
      .filter(res => res.getName == "ports").iterator.next().getRanges.getRange(0).getBegin.toInt
    val agentHost = offer.getHostname
    val dockerImage = "trinitronx/python-simplehttpserver"

    val task = TaskInfo.newBuilder
      .setName("test")
      .setTaskId(TaskID.newBuilder
        .setValue(reqs.taskId))
      .setAgentId(offer.getAgentId)
      .setCommand(CommandInfo.newBuilder
        .setEnvironment(Environment.newBuilder
          .addVariables(Environment.Variable.newBuilder
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
```

* init the client:
```scala
    val mesosClientActor = system.actorOf(MesosClientActor.props(
      "whisk-invoker-"+UUID.randomUUID(),
      "whisk-framework",
      "http://192.168.99.100:5050",
      "*",
      taskBuilder = buildTask
    ))

    //use ask pattern to wait for Subscribe to complete:
    mesosClientActor.ask(Subscribe)(subscribeTimeout).mapTo[SubscribeComplete].onComplete(complete => {
      log.info("subscribe completed successfully...")
    })
```
* run a mesos master+agent:
```bash
DOCKER_IP=192.168.99.100 docker-compose -f docker-compose-mesos.yml up
```
* run your application



