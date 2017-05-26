#!/bin/bash

OVS_BRIDGE='ovsbr0'

i=$1
p=`printf %02d $i`;
IPADDR=10.0.0.${i}

echo "Node: $IPADDR" && \
rm -fr /data/docker_volumes/v${p} && mkdir -p /data/docker_volumes/v${p} && \
chown 999.999 /data/docker_volumes/v${p} && chmod 777 /data/docker_volumes/v${p} && \
sleep $[80+$i] && \
docker run -d -e CASSANDRA_SEEDS=10.0.0.1 -e CASSANDRA_LISTEN_ADDRESS=${IPADDR} --privileged -v /data/docker_volumes/v${p}:/var/lib/cassandra --user=cassandra --entrypoint="/bin/bash" --memory=5g --memory-swap=0 --memory-swappiness=0 infwonder/cassandra-g1gc:3.10 -c "sleep 3 && /docker-entrypoint.sh cassandra -f" |cut -c -12|xargs -l -i ovs-docker add-port ovsbr0 eth1 {} --ipaddress=${IPADDR}/16

echo "Done";
