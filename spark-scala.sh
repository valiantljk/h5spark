#!/bin/bash


#SBATCH -p debug
#SBATCH -N 3
#SBATCH -t 00:10:00
#SBATCH -e mysparkjob_%j.err
#SBATCH -o mysparkjob_%j.out
#SBATCH --ccm
#SBATCH --volume="/global/cscratch1/sd/jialin/spark_tmp_dir/climate:/tmp:perNodeCache=size=200G"
module unload spark/hist-server
module load spark
module load collectl
start-collectl.sh 
start-all.sh

# to create a fat jar
# sbt assembly
# test the multiple hdf5 file reader:
export SPARK_LOCAL_DIRS="/tmp"
export LD_LIBRARY_PATH=$LD_LBRARY_PATH:$PWD/lib
###load single large hdf5 file####
partition="1"
repartition="10"
inputfile="/global/cscratch1/sd/jialin/climate/oceanTemps.hdf5"
dataset="temperatures"
rows="6349676"
type="64"
#csvlist="src/resources/hdf5/oceanlist.csv"
csvlist="src/resources/hdf5/oceanlist10.csv"
spark-submit --verbose\
  --master $SPARKURL\
  --driver-memory 100G\
  --executor-cores 5 \
  --driver-cores 32  \
  --num-executors=2  \
  --executor-memory 100G\
  --class org.nersc.io.read\
  --conf spark.eventLog.enabled=true\
  --conf spark.eventLog.dir=$SCRATCH/spark/spark_event_logs\
  target/scala-2.10/h5spark-assembly-1.0.jar \
  $csvlist $partition $repartition $inputfile $dataset $rows 

#$csvlist $partition $repartition $inputfile $dataset $rows
# check history server information####
# module load spark/hist-server
# ./run_history_server.sh $EVENT_LOGS_DIR 

stop-all.sh
stop-collectl.sh
