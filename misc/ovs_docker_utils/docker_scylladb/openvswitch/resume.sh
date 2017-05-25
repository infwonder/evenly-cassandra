#!/bin/bash

cid=$1
i=$2

ovs-docker del-ports ovsbr0 $cid && \
docker start $cid && ovs-docker add-port ovsbr0 eth1 $cid --ipaddress=10.0.0.${i}/16
