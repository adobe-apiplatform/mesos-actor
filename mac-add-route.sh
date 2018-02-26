#!/bin/bash
set -euo pipefail
addOrDelete=${1:-add}

MACHINE_NAME=${2:-default}
gateway=$(docker-machine ip $MACHINE_NAME)

gateway=$(docker-machine ip)
networkname=$(echo $(basename $(pwd)) | tr -d '[:punct:]')_default
subnet=$(docker network inspect mesosactor_default -f '{{(index .IPAM.Config 0).Subnet}}')

if [ "$addOrDelete" = "add" ]; then
  sudo route -n add -net ${subnet} ${gateway}
  exit 0
fi

if [ "$addOrDelete" = "rm" ]; then
 sudo route -n delete -net ${subnet} ${gateway}
 exit 0
fi

echo "Unknown command, please only use rm"
exit 1
