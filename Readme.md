# mesos-client-scala

A lightweight Mesos meta-framework for assembling Mesos Frameworks (schedulers). 
Based on:
* Akka
* Akka Streams
* Akka HTTP
* Mesos scheduler HTTP API [http://mesos.apache.org/documentation/latest/scheduler-http-api/]


# Example

See `src/main/scala/com/adobe/api/platform/runtime/mesos/sample/SampleFramework.scala` for an example framework that:
* initiates the Mesos Client subscription
* submits a task for execution
* kills the task after some time
* shuts down the Mesos Client on application termination 

# Usage

To use in your own application:
* add dependencies to your app
```text
    //for mesos actor
    compile "com.adobe.api.platform.runtime:mesos:0.0.1"
```

* implement a custom task matcher, if desired

This trait is used to match pending tasks with received offers.

Implement the `com.adobe.api.platform.runtime.mesos.TaskMatcher` trait
Default is `com.adobe.api.platform.runtime.mesos.DefaultTaskMatcher`

* implement a custom task builder, if desired

This trait is used to build TaskInfo objects for Offers that have been matched.

Implement the `com.adobe.api.platform.runtime.mesos.TaskBuilder` trait
Default is `com.adobe.api.platform.runtime.mesos.DefaultTaskBuilder`


* init the client:
```scala
    val mesosClientActor = system.actorOf(MesosClient.props(
        "sample-" + UUID.randomUUID(),
        "sample-framework",
        "http://192.168.99.100:5050",
        "*",
        yourTaskMatcher, //optional
        yourTaskBuilder  //optional
    ))

    //use ask pattern to wait for Subscribe to complete:
    mesosClientActor.ask(Subscribe)(subscribeTimeout).mapTo[SubscribeComplete].onComplete(complete => {
        log.info("subscribe completed successfully...")
    })
```

# Running

You will need a mesos master and one or more mesos agents running.

* run a mesos master+agent using docker-compose:
```bash
DOCKER_IP=192.168.99.100 docker-compose up
```
* run your application 



