#!/bin/bash


#SBATCH -p regular
#SBATCH -N 46 
#SBATCH -t 00:05:00
#SBATCH -e mysparkjob_%j.err
#SBATCH -o mysparkjob_%j.out
#SBATCH --ccm
#SBATCH --qos=premium
#module unload spark/hist-server
module load spark
#module load collectl
#start-collectl.sh 
start-all.sh

export LD_LIBRARY_PATH=$LD_LBRARY_PATH:$PWD/lib

###load single large hdf5 file####
repartition="2880"
inputfile="/global/cscratch1/sd/jialin/dayabay/ost248/oceanTemps.hdf5"
app_name="H5Sspark-mpi-ost248"
dataset="temperatures"

spark-submit --verbose\
  --master $SPARKURL\
  --name $app_name \
  --driver-memory 100G\
  --executor-cores 32 \
  --driver-cores 32  \
  --num-executors=45 \
  --executor-memory 105G\
  --class org.nersc.io.readtest\
  --conf spark.eventLog.enabled=true\
  --conf spark.eventLog.dir=$SCRATCH/spark/spark_event_logs\
  target/scala-2.10/h5spark-assembly-1.0.jar \
  $repartition $inputfile $dataset 


rm /global/cscratch1/sd/jialin/spark_tmp_dir/*
stop-all.sh
#stop-collectl.sh
