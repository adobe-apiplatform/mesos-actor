#!/bin/bash
docker-compose ps -q master | xargs -L1 -d "\n" docker inspect -f "{{.Name}}   :  {{.NetworkSettings.Networks.mesosactor_default.IPAddress}}"
docker-compose ps -q marathon | xargs -L1 -d "\n" docker inspect -f "{{.Name}}   : {{.NetworkSettings.Networks.mesosactor_default.IPAddress}}"
docker-compose ps -q slave | xargs -L1 -d "\n" docker inspect -f "{{.Name}}   : {{.NetworkSettings.Networks.mesosactor_default.IPAddress}}"
docker-compose ps -q zk | xargs -L1 -d "\n" docker inspect -f "{{.Name}}   : {{.NetworkSettings.Networks.mesosactor_default.IPAddress}}"
