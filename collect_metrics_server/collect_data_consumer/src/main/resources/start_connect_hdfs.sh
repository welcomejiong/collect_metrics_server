#! /bin/bash

nohup  java -Djava.ext.dirs=/home/services/connecthdfs/lib/:$JAVA_HOME/jre/lib/ext -cp connect_consumer_to_hdfs-8.3.0-SNAPSHOT.jar -Xms100M -Xmx2048M -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70 -XX:+CMSParallelRemarkEnabled  org.corps.bi.datacenter.connect.ConnectMain > ./nohup_connect_hdfs_run.log  2>&1 & 