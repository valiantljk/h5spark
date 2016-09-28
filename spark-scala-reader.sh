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

# Test for HDF4.
# inputfile="/tmp/3B43.070901.6A.HDF"
# offset=294
# length=4
# decompress=0

# Test for HDF5.
# Run the following command on Jam to obtain offset/length information.
# $/mnt/hdf/kent/hdf5-1.8.17-stinfo/examples/h5dstoreinfo ~/NASAHDF/oco2_L2StdND_03945a_150330_B6000_150331024816.h5 /L1bScSoundingReference/packaging_qual_flag
# It's an unsiged integer 8.
inputfile="/tmp/oco2_L2StdND_03945a_150330_B6000_150331024816.h5"
offset=108875990
length=1086
decompress=1

app_name="H5Sspark-reader"
spark-submit --verbose\
  --master $SPARKURL\
  --name $app_name \
  --driver-memory 100G\
  --executor-cores 32 \
  --driver-cores 32  \
  --num-executors=5 \
  --executor-memory 105G\
  --class org.hdfgroup.spark.HDFByteReader\
  --conf spark.eventLog.enabled=true\
  --conf spark.eventLog.dir=$SCRATCH/spark/spark_event_logs\
  target/scala-2.11/h5spark-assembly-1.0.jar \
  "$inputfile" $offset $length $decompress


