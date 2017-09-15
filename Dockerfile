#
# Scala and sbt Dockerfile
#
# https://github.com/hseeberger/scala-sbt
#

# Pull base image
FROM  openjdk:8-jdk-alpine

ENV SCALA_VERSION 2.11.11

# Install Scala
## Piping curl directly in tar
RUN \
  apk update && apk add curl && \
  curl -fsL https://downloads.typesafe.com/scala/$SCALA_VERSION/scala-$SCALA_VERSION.tgz | tar xfz - -C /root/ && \
  echo >> /root/.bashrc && \
  echo 'export PATH=~/scala-$SCALA_VERSION/bin:$PATH' >> /root/.bashrc

# Scala expects this file
# RUN touch /usr/lib/jvm/java-8-openjdk-amd64/release

#
# Copy app jars
ADD build/distributions/mesos-actor-0.0.3.tar ./
ENV MESOS_ACTOR_OPTS -Dconfig.resource=application_ha.conf
ENV LIBPROCESS_IP 0.0.0.0
ENV HOST 0.0.0.0
ENV PORT_2551 2551

CMD ./mesos-actor-0.0.3/bin/mesos-actor

EXPOSE 8080
