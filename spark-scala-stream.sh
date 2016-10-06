#!/bin/bash
# Copyright (C) 2016 The HDF Group
# All rights reserved
#
# This example shows how to submit spark job for streaming test.
# This is modifeid shell script based on spark-scala.sh.
# This script is tested on Mac OS X Mavericks machine with the following:
#
# Hadoop 2.7.3
# Spark 2.0.0 with Hadoop 2.7.

SPARKURL="local[1]"
SCRATCH="/tmp"
inputfile="hdfs://jaguar:9000/stream/"
app_name="H5Sspark-stream"
spark-submit --verbose\
  --master $SPARKURL\
  --name $app_name \
  --driver-memory 100G\
  --executor-cores 32 \
  --driver-cores 32  \
  --num-executors=5 \
  --executor-memory 105G\
  --class org.hdfgroup.spark.HdfsByteCount\
  --conf spark.eventLog.enabled=true\
  --conf spark.eventLog.dir=$SCRATCH/spark/spark_event_logs\
  target/scala-2.11/h5spark-assembly-1.0.jar \
  "$inputfile"


