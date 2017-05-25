#!/bin/bash

seed_node=$1
begin_node=$2
end_node=$3

for i in `seq $begin_node $end_node`; do
  if [ $[$i % 3] == 0 ]; then
    echo "10.0.0.${i} will be deployed on mesos01";
    ssh root@mesos01 "sh ~/docker_cassandra/openvswitch/add_slave_nowait.sh $i"
  elif [ $[$i % 3] == 1 ]; then
    echo "10.0.0.${i} will be deployed on pluto";
    sh ~/docker_cassandra/openvswitch/add_slave_nowait.sh $i
  elif [ $[$i % 3] == 2 ]; then
    echo "10.0.0.${i} will be deployed on mesos02";
    ssh root@mesos02 "sh ~/docker_cassandra/openvswitch/add_slave_nowait.sh $i"
  fi

  hold=1;
  while [ $hold -eq "1" ]; do
    hold=`docker logs ${seed_node} 2>&1|tail -n 10|grep "Session with /10.0.0.${i} is complete" &> /dev/null && echo 0 || echo 1`;
    sleep 0.0001;
  done
  	
done
  
