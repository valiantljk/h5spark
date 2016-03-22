#!/bin/bash


#SBATCH -p debug
#SBATCH -N 2
#SBATCH -t 00:10:00
#SBATCH -e mysparkjob_%j.err
#SBATCH -o mysparkjob_%j.out
#SBATCH --ccm
module unload spark/hist-server
module load spark
module load collectl
start-collectl.sh 
start-all.sh

# to create a fat jar
# sbt assembly
# test the multiple hdf5 file reader:
export LD_LIBRARY_PATH=$LD_LBRARY_PATH:lib/
spark-submit --master $SPARKURL --driver-memory 80G --executor-memory 80G --class org.nersc.io.read --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=$SCRATCH/spark/spark_event_logs target/scala-2.10/h5spark-assembly-1.0.jar


# check history server information####
# module load spark/hist-server
# ./run_history_server.sh $EVENT_LOGS_DIR 

stop-all.sh
stop-collectl.sh
