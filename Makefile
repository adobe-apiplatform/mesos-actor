.PHONY: docker
docker:
	docker build -t adobeapiplatform/mesos-actor .

scala:
	./gradlew build

.PHONY: all
all: scala docker

.PHONY: compose
compose:
	DOCKER_IP=`docker-machine ip default` docker-compose up

