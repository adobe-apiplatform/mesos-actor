#!/bin/bash
while true; do docker-compose up -d --scale slave=3 --scale master=3; echo @$(date +%s); sleep 1m;done
