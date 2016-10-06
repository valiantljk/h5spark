#!/bin/bash
# Copyright (C) 2016 The HDF Group
# All rights reserved
#
# This example shows how to submit spark job using Yarn in cluster mode.
# This is modifeid shell script based on spark-scala.sh.
# This script is tested on Mac OS X Mavericks machine with the following:
#
# Hadoop 2.7.3
# Spark 2.0.0 with Hadoop 2.7.
#
export JAVA_LIBRARY_PATH=$JAVA_LIBRARY_PATH:/Users/hyoklee/Library/Java/Extensions

# This is critical for Yarn to load HDF5 JNI library properly. 
export SPARK_YARN_USER_ENV="JAVA_LIBRARY_PATH=$JAVA_LIBRARY_PATH,LD_LIBRARY_PATH=$LD_LIBRARY_PATH"
export HADOOP_MAPRED_HOME=/Users/hyoklee/src/hadoop-2.7.3/

# This is useful to check java.library.path.
java -XshowSettings:properties -version

repartition="1"

# A NASA HDF-EOS5 file
inputfile="/tmp/GSSTF_NCEP.3.1987.07.01.he5"
app_name="H5Sspark-udf"
dataset="/HDFEOS/GRIDS/NCEP/Data Fields/SST"

# For standalone single node
# SPARKURL="local[1]"

# For standalone cluster
# SPARKURL="spark://localhost:7077"

# For Yarn
SPARKURL="yarn"
SCRATCH="/tmp"
spark-submit --verbose\
             --master $SPARKURL\
             --deploy-mode cluster \
             --name $app_name \
             --executor-cores 1 \
             --driver-cores 1  \
             --num-executors=1 \
             --class org.nersc.io.readtest\
             --conf spark.eventLog.enabled=true\
             --conf spark.eventLog.dir=$SCRATCH/spark\
             --driver-library-path=.\
             target/scala-2.11/h5spark-assembly-1.0.jar \
             $repartition "$inputfile" "$dataset"
rm /tmp/spark/*
# stop-all.sh
# stop-collectl.sh
