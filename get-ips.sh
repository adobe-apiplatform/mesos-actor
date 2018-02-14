#!/bin/bash
docker-compose ps -q master | xargs docker inspect -f "{{.Name}}   :  {{.NetworkSettings.Networks.mesosactor_default.IPAddress}}"
docker-compose ps -q marathon | xargs docker inspect -f "{{.Name}}   : {{.NetworkSettings.Networks.mesosactor_default.IPAddress}}"
docker-compose ps -q slave | xargs docker inspect -f "{{.Name}}   : {{.NetworkSettings.Networks.mesosactor_default.IPAddress}}"
docker-compose ps -q zk | xargs docker inspect -f "{{.Name}}   : {{.NetworkSettings.Networks.mesosactor_default.IPAddress}}"
