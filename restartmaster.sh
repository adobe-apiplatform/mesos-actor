#!/bin/bash
#minimesos does not monitor itself to make sure master (and mesos-dns FWIW) is running after returning from sleep...)

docker start $(docker ps -aqf name=mesos-master) && docker start $(docker ps -aqf name=mesosdns)
