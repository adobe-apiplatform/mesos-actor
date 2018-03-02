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

import akka.NotUsed
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding.Post
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.unmarshalling.FromEntityUnmarshaller
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.http.scaladsl.unmarshalling.Unmarshaller
import akka.stream.ActorMaterializer
import akka.stream.Attributes
import akka.stream.FlowShape
import akka.stream.Inlet
import akka.stream.Outlet
import akka.stream.OverflowStrategy
import akka.stream.alpakka.recordio.scaladsl.RecordIOFraming
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.InHandler
import akka.stream.stage.OutHandler
import akka.util.ByteString
import java.util.Optional
import org.apache.mesos.v1.Protos.FrameworkID
import org.apache.mesos.v1.Protos.FrameworkInfo
import org.apache.mesos.v1.scheduler.Protos.Call
import org.apache.mesos.v1.scheduler.Protos.Event
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.Failure
import scala.util.Success

trait MesosClientHttpConnection extends MesosClientConnection {
    this: MesosClientActor =>
    implicit val materializer:ActorMaterializer = ActorMaterializer()

    val poolClientFlow = Http().cachedHostConnectionPool[Promise[HttpResponse]](host = mesosUri.getHost, port = mesosUri.getPort)
    val queue =
        Source.queue[(HttpRequest, Promise[HttpResponse])](100, OverflowStrategy.backpressure)
                .via(poolClientFlow)
                .toMat(Sink.foreach({
                  case ((Success(resp), p)) => p.success(resp)
                  case ((Failure(e), p))    => p.failure(e)
                }))(Keep.left)
                .run()

    def exec(call: Call): Future[HttpResponse] = {
        log.info(s"sending ${call.getType}")
        val req = Post("/api/v1/scheduler")
                .withHeaders(
                    RawHeader("Mesos-Stream-Id", streamId))
                .withEntity(protobufContentType, call.toByteArray)
        val responsePromise = Promise[HttpResponse]()
        queue.offer(req, responsePromise)
        responsePromise.future
    }


    def subscribe(frameworkID: FrameworkID, frameworkName: String, failoverTimeoutSecond: Double): Future[SubscribeComplete] = {

        import EventStreamUnmarshalling._

        val result = Promise[SubscribeComplete]

        val subscribeCall = Call.newBuilder()
                .setType(Call.Type.SUBSCRIBE)
                .setFrameworkId(frameworkID)
                .setSubscribe(Call.Subscribe.newBuilder
                        .setFrameworkInfo(FrameworkInfo.newBuilder
                                .setId(frameworkID)
                                .setUser(Optional.ofNullable(System.getenv("user")).orElse("root")) // https://issues.apache.org/jira/browse/MESOS-3747
                                .setName(frameworkName)
                                 .setFailoverTimeout(failoverTimeoutSeconds.toSeconds)
                                .setRole(role)
                                .build)
                        .build())
                .build()
        logger.info(s"sending SUBSCRIBE to ${mesosUri}")
        //TODO: handle connection failures: http://doc.akka.io/docs/akka-http/10.0.5/scala/http/low-level-server-side-api.html
        //see https://gist.github.com/ktoso/4dda7752bf6f4393d1ac
        //see https://tech.zalando.com/blog/about-akka-streams/?gh_src=4n3gxh1
        Http()
                .singleRequest(Post(s"${mesosUri}/subscribe")
                        .withHeaders(RawHeader("Accept", "application/x-protobuf"),
                            RawHeader("Connection", "close"))
                        .withEntity(protobufContentType, subscribeCall.toByteArray))
                .flatMap(response => {
                    if (response.status.isSuccess()) {
                        logger.debug(s"response: ${response} ")
                        val newStreamId = response.getHeader("Mesos-Stream-Id").get().value()
                        if (streamId == null) {
                            logger.info(s"setting new streamId ${newStreamId}")
                            streamId = newStreamId
                            result.success(SubscribeComplete(frameworkID.getValue))
                        } else if (streamId != newStreamId) {
                            //TODO: do we need to handle StreamId changes?
                            logger.warning(s"streamId has changed! ${streamId}  ${newStreamId}")
                        }
                        Unmarshal(response).to[Source[Event, NotUsed]]
                    } else {
                        //TODO: reconnect?
                        throw new Exception(s"subscribe response failed: ${response}")
                    }
                })

                .foreach(eventSource => {
                    eventSource.runForeach(event => {
                        handleEvent(event)
                    })
                })



        result.future
    }
}

//see https://github.com/hseeberger/akka-sse
object EventStreamUnmarshalling extends EventStreamUnmarshalling

trait EventStreamUnmarshalling {

    implicit final val fromEventStream: FromEntityUnmarshaller[Source[Event, NotUsed]] = {
        val eventParser = new ServerSentEventParser()

        def unmarshal(entity: HttpEntity) =

            entity.withoutSizeLimit.dataBytes
                    .via(RecordIOFraming.scanner())
                    .via(eventParser)
                    .mapMaterializedValue(_ => NotUsed: NotUsed)

        Unmarshaller.strict(unmarshal).forContentTypes(protobufContentType)
    }
}

private final class ServerSentEventParser() extends GraphStage[FlowShape[ByteString, Event]] {

    override val shape =
        FlowShape(Inlet[ByteString]("ServerSentEventParser.in"), Outlet[Event]("ServerSentEventParser.out"))

    override def createLogic(attributes: Attributes) =
        new GraphStageLogic(shape) with InHandler with OutHandler {

            import shape._

            setHandlers(in, out, this)

            override def onPush() = {
                val line: ByteString = grab(in)
                //unmarshall proto
                val event = Event.parseFrom(line.toArray)
                push(out, event)
            }

            override def onPull() = pull(in)
        }
}