#!/bin/bash


#SBATCH -p debug
#SBATCH -N 2
#SBATCH -t 00:30:00
#SBATCH -e mysparkjob_%j.err
#SBATCH -o mysparkjob_%j.out
#SBATCH --ccm
module unload spark/hist-server
module load spark
module unload python
module load python/2.7-anaconda

 
start-all.sh

spark-submit --master $SPARKURL --driver-memory 15G --executor-memory 32G --class org.apache.spark.mllib.linalg.distributed.netCDFTest --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=$SCRATCH/spark/spark_event_logs ./tests/read_tests.py

#module load spark/hist-server
#./run_history_server.sh $EVENT_LOGS_DIR 

stop-all.sh
