# mesos-actor

A lightweight Mesos meta-framework for assembling Mesos Frameworks (schedulers). 
Based on:
* Akka
* Akka Streams
* Akka HTTP
* Akka Clustering
* Mesos scheduler HTTP API [http://mesos.apache.org/documentation/latest/scheduler-http-api/]


# Example

See [SampleFramework.scala](./src/main/scala/com/adobe/api/platform/runtime/mesos/sample/SampleFramework.scala) for an example framework that:
* initiates the Mesos Client subscription
* submits some tasks for execution
* kills the tasks after some time
* shuts down the Mesos Client on application termination 

# Usage

To use in your own application:
* add dependencies to your app

Gradle:

```text
    //for mesos-actor
    compile "com.adobe.api.platform.runtime:mesos-actor:0.0.1"
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
        "sample-" + UUID.randomUUID(), //start with a new id (TODO: persist this id so restart will fail over to new instance)
        "sample-framework",
        "http://192.168.99.100:5050", //your mesos master ip
        "sample-role", //role for this framework
        30.seconds, //failover timeout      
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

* run a mesos master+agent using docker-compose (tested with docker-machine; specify the IP of your docker-machine VM):
```bash
DOCKER_IP=192.168.99.100 docker-compose up
```
* run your application 

# HA 

For a highly available framework, multiple instances of the framework must be deployed:
* Only one of those instances should be the leader
* Only the leader should subscribe to Mesos
* When the leader instance dies, another instance should take the leadership role
* The new leader should create a new subscription to Mesos master using the same framework id
* The new leader should reconcile exising tasks, and resume managing the tasks started previously


See [SampleHAFramework.scala](./src/main/scala/com/adobe/api/platform/runtime/mesos/sample/SampleHAFramework.scala) for an example.

## Running an HA example

### Build the project
```bash
$ make all
```

This command builds the sample and a docker image that can be deployed in Mesos via Marathon.
 
### Start a Mesos Cluster

```bash
$ DOCKER_IP=192.168.99.100 docker-compose up 
``` 

> NOTE: the command above assumes that `docker-machine`'s IP is: `192.168.99.100`

Browse to: 
* http://192.168.99.100:5050/
* http://192.168.99.100:8080/

### Deploy the framework via Marathon:

```bash
$ curl http://192.168.99.100:8080/v2/apps/ --data @./marathon-config/marathon-app-local.json -H "Content-type: application/json"
```

After a short while the Mesos UI should display a few tasks running, and a new framework should be registered.   

### Testing HA

Restart the Marathon app: 

```bash
$ curl -X POST http://192.168.99.100:8080/v2/apps/akka-cluster/restart
```

Marathon should start new instances, wait until they become healthy, and then destroy the previous ones. 
When the previous leader is destroyed, the framework should show as `inactive` in Mesos. 
Once a new leader is selected, the framework should then show back as `active`.  
