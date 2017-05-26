#!/bin/bash

# weave launch
# eval $(weave env)

i=$1
p=`printf %02d $i`;
IPADDR=10.0.0.${i}

echo "Node: $IPADDR" && \
rm -fr /data/docker_volumes/v${p} && mkdir -p /data/docker_volumes/v${p} && \
chown 999.999 /data/docker_volumes/v${p} && chmod 777 /data/docker_volumes/v${p} && \
sleep $[80+$i] && \
docker run -d -e WEAVE_CIDR=${IPADDR}/24 -e CASSANDRA_SEEDS=10.0.0.1 --memory=4g --memory-swap=0 --memory-swappiness=0 --privileged --user=cassandra --entrypoint="/docker-entrypoint.sh" -v /data/docker_volumes/v${p}:/var/lib/cassandra infwonder/cassandra-g1gc:3.10 cassandra -f

echo "Done";
