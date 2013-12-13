#!/bin/sh

killall -9 java

# delete PID files
rm /Data1/Code/cassandra/cassandra-dev/cluster/node1/cassandra.pid
rm /Data1/Code/cassandra/cassandra-dev/cluster/node2/cassandra.pid

# delete logs
rm -rf /Data1/Code/cassandra/cassandra-dev/cluster/node1/logs/*
rm -rf /Data1/Code/cassandra/cassandra-dev/cluster/node2/logs/*

# delete data files
rm -rf /Data1/Code/cassandra/cassandra-dev/cluster/node1/data/*
rm -rf /Data1/Code/cassandra/cassandra-dev/cluster/node2/data/*

# delete commitlogs
rm -rf /Data1/Code/cassandra/cassandra-dev/cluster/node1/commitlog/*
rm -rf /Data1/Code/cassandra/cassandra-dev/cluster/node2/commitlog/*

# delete saved_caches
rm -rf /Data1/Code/cassandra/cassandra-dev/cluster/node1/saved_caches/*
rm -rf /Data1/Code/cassandra/cassandra-dev/cluster/node2/saved_caches/*

ant build

# run node1
export CASSANDRA_CONF=/Data1/Code/cassandra/cassandra-dev/cluster/node1/conf
./bin/cassandra -p/Data1/Code/cassandra/cassandra-dev/cluster/node1/cassandra.pid -Dcassandra.config=file:/Data1/Code/cassandra/cassandra-dev/cluster/node1/conf/cassandra.yaml -Dlog4j.configuration=file:/Data1/Code/cassandra/cassandra-dev/cluster/node1/conf/log4j-server.properties 

#run node 2
export CASSANDRA_CONF=/Data1/Code/cassandra/cassandra-dev/cluster/node2/conf
./bin/cassandra -p/Data1/Code/cassandra/cassandra-dev/cluster/node2/cassandra.pid -Dcassandra.config=file:/Data1/Code/cassandra/cassandra-dev/cluster/node2/conf/cassandra.yaml -Dlog4j.configuration=file:/Data1/Code/cassandra/cassandra-dev/cluster/node2/conf/log4j-server.properties 



