#!/bin/bash

OVS_BRIDGE='ovsbr0'

i=$1
p=`printf %02d $i`;
IPADDR=10.0.0.${i}

echo "Node: $IPADDR" && \
rm -fr /data/docker_volumes/v${p} && mkdir -p /data/docker_volumes/v${p} && \
chown 999.999 /data/docker_volumes/v${p} && chmod 777 /data/docker_volumes/v${p} && \
docker run -d --privileged -v /data/docker_volumes/v${p}:/var/lib/scylla --entrypoint="/bin/bash" --memory=8448M --memory-swap=0 --memory-swappiness=0 scylladb:latest -c "sleep 3 && /docker-entrypoint.py --listen-address $IPADDR --seeds 10.0.0.1 --memory 8G --overprovisioned 1 --developer-mode 1" |cut -c -12|xargs -l -i ovs-docker add-port ovsbr0 eth1 {} --ipaddress=${IPADDR}/24

echo "Done";
