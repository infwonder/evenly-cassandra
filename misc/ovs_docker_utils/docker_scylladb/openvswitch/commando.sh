#!/bin/bash

seed_node=$1
begin_node=$2
end_node=$3

for i in `seq $begin_node $end_node`; do
  if [ $[$i % 3] == 0 ]; then
    echo "10.0.0.${i} will be deployed on mesos01";
    ssh root@mesos01 "sh ~/docker_scylladb/openvswitch/add_slave_nowait.sh $i"
  elif [ $[$i % 3] == 1 ]; then
    echo "10.0.0.${i} will be deployed on pluto";
    sh ~/docker_scylladb/openvswitch/add_slave_nowait.sh $i
  elif [ $[$i % 3] == 2 ]; then
    echo "10.0.0.${i} will be deployed on mesos02";
    ssh root@mesos02 "sh ~/docker_scylladb/openvswitch/add_slave_nowait.sh $i"
  fi

  hold=1;
  while [ $hold -eq "1" ]; do
    hold=`docker logs ${seed_node} 2>&1|tail -n 10|grep "All sessions completed for streaming plan Bootstrap, peers={10.0.0.${i}}" &> /dev/null && echo 0 || echo 1`;
    sleep 0.0001;
  done
  	
done
  
