#!/bin/bash

if [ -z "$1" ];
  then
      servers=5
  else
      servers=$1
fi

if [ -z "$2" ];
  then
      clients=2
  else
      clients=$2
fi

echo "Clients: ${clients}"
echo "Servers: ${servers}"

#gnome-terminal  -- bash -c 'cd `pwd`; python3 raft_setup.py; exec bash'
gnome-terminal  -- bash -c 'cd `pwd`; python3 raft_setup.py; '

for serve in $(seq 1 $servers);
do
  #echo "server: ${serve}"
  #echo "cd `pwd`; python3 server.py configs/server-${serve}.json; exec bash"
  gnome-terminal  -- bash -c "cd `pwd`; python3 server.py configs/server-${serve}.json; exec bash"
done

for client in $(seq 1 $clients);
do
   #echo "client: ${client}"
   #echo "cd `pwd`; python3 client.py configs/client-${client}.json; exec bash"
   gnome-terminal  -- bash -c "cd `pwd`; python3 client.py configs/client-${client}.json; exec bash"
done