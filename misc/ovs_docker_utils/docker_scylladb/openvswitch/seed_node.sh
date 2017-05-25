#!/bin/bash

OVS_BRIDGE='ovsbr0'

# seed node
echo "Seed Node: 10.0.0.1";
rm -fr /data/docker_volumes/v01 && mkdir -p /data/docker_volumes/v01 && \
chown 999.999 /data/docker_volumes/v01 && chmod 777 /data/docker_volumes/v01 && \
docker run -d --privileged --memory=8448M --memory-swap=0 --memory-swappiness=0 --entrypoint="/bin/bash" -v /data/docker_volumes/v01:/var/lib/scylla scylladb:latest -c "sleep 3 && /docker-entrypoint.py --listen-address 10.0.0.1 --broadcast-address 10.0.0.1 --broadcast-rpc-address 10.0.0.1 --memory 8G --overprovisioned 1 --developer-mode 1" |cut -c -12|xargs -l -i ovs-docker add-port ovsbr0 eth1 {} --ipaddress=10.0.0.1/24

echo "Done";
