#!/bin/bash

OVS_BRIDGE='ovsbr0'

# seed node
echo "Seed Node: 10.0.0.1";
rm -fr /data/docker_volumes/v01 && mkdir -p /data/docker_volumes/v01 && \
chown 999.999 /data/docker_volumes/v01 && chmod 777 /data/docker_volumes/v01 && \
docker run -d -e CASSANDRA_BROADCAST_ADDRESS=10.0.0.1 -e CASSANDRA_LISTEN_ADDRESS=10.0.0.1 --privileged --memory=8g --memory-swap=0 --memory-swappiness=0 --user=cassandra --entrypoint="/bin/bash" -v /data/docker_volumes/v01:/var/lib/cassandra infwonder/cassandra-g1gc:3.10 -c "sleep 3 && /docker-entrypoint.sh cassandra -f" |cut -c -12|xargs -l -i ovs-docker add-port ovsbr0 eth1 {} --ipaddress=10.0.0.1/16

echo "Done";
