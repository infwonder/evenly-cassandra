#!/bin/bash

# weave launch
# eval $(weave env)

#MODE=$1
#tout=30;

#if [ $MODE == 'init' ]; then
#  tout=65;
#fi   

# seed node
echo "Seed Node: 10.0.0.1";
docker run -d -e WEAVE_CIDR=10.0.0.1/24 -e CASSANDRA_BROADCAST_ADDRESS=10.0.0.1 -e CASSANDRA_LISTEN_ADDRESS=10.0.0.1 --memory=4g --memory-swap=0 --memory-swappiness=0 --privileged --user=cassandra --entrypoint="/docker-entrypoint.sh" -v /data/docker_volumes/v01:/var/lib/cassandra infwonder/cassandra-g1gc:3.10 cassandra -f

#for i in `seq 2 7`; do
#  echo "Node: 10.0.0.$i" && \
#  docker run -d -e WEAVE_CIDR=10.0.0.${i}/24 -e CASSANDRA_SEEDS=10.0.0.1 --memory=4g --memory-swap=0 --memory-swappiness=0 --privileged --user=cassandra --entrypoint="/docker-entrypoint.sh" -v /data/docker_volumes/v0${i}:/var/lib/cassandra infwonder/cassandra-g1gc:3.10 cassandra -f
#  sleep $[$tout+$i]
#done 

echo "Done";
