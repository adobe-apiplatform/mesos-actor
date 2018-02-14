#!/bin/bash

gateway=$(docker-machine ip)
networkname=$(echo $(basename $(pwd)) | tr -d '[:punct:]')_default
subnet=$(docker network inspect mesosactor_default -f '{{(index .IPAM.Config 0).Subnet}}')
sudo route -n add -net ${subnet} ${gateway}
#sudo route -n delete -net ${subnet} ${gateway}
