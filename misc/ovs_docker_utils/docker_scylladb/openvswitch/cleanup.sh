#!/bin/bash

docker ps |grep scylla|awk '{print $1}' |xargs -l -i docker stop {} && \
for i in `docker ps --all|grep scylla|awk '{print $1}'`; do docker rm $i; ovs-docker del-ports ovsbr0 $i; done && \
echo "--------------------------------" && \
docker ps --all && \
echo "--------------------------------" && \
ovs-vsctl show
