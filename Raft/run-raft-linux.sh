#!/bin/bash

if [ -z "$1" ];
  then
      qty_servers=5
  else
      qty_servers=$1
fi

if [ -z "$2" ];
  then
      servers=5
  else
      servers=$2
fi

if [ -z "$3" ];
  then
      clients=2
  else
      clients=$3
fi


echo "Servers in the whole system: ${qty_servers}"
echo "Servers running: ${servers}"
echo "Clients running: ${clients}"

#gnome-terminal  -- bash -c 'cd `pwd`; python3 raft_setup.py; exec bash'
gnome-terminal  -- bash -c "cd `pwd`; python3 raft_setup.py ${qty_servers} ${clients}; "

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