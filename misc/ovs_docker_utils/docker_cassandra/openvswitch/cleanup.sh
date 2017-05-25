#!/bin/bash

docker ps |grep cassandra|awk '{print $1}' |xargs -l -i docker stop {} && \
for i in `docker ps --all|grep cassandra|awk '{print $1}'`; do docker rm $i; ovs-docker del-ports ovsbr0 $i; done && \
docker ps --all && ovs-vsctl show
