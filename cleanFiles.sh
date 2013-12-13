#!/bin/sh

killall -9 java

rm -rf /var/log/cassandra/*
rm -rf  /var/lib/cassandra/saved_caches/*
rm -rf  /var/lib/cassandra/data/*
rm -rf  /var/lib/cassandra/commitlog/*

ant build
