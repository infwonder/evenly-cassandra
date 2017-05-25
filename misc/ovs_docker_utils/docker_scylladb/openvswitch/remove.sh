#!/bin/bash

cid=$1

docker rm $cid && ovs-docker del-ports ovsbr0 $cid
