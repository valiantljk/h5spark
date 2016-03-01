#!/bin/bash


#SBATCH -p debug
#SBATCH -N 10
#SBATCH -t 00:30:00
#SBATCH -e mysparkjob_%j.err
#SBATCH -o mysparkjob_%j.out
#SBATCH --ccm
module unload spark/hist-server
module load spark
module unload python
module load python/2.7-anaconda
module load collectl
start-collectl.sh 
start-all.sh

####load multiple hdf5 files###

#spark-submit --master $SPARKURL --executor-cores 10 --driver-memory 20G --executor-memory 60G --class org.apache.spark.mllib.linalg.distributed.netCDFTest --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=$SCRATCH/spark/spark_event_logs ./tests/read_tests-app.py /global/homes/j/jialin/spark-io/h5spark/python/tests/resources/hdf5/filelist40000 1 100

###load single large hdf5 file####
spark-submit --master $SPARKURL --executor-cores 30 --driver-memory 120G --executor-memory 100G --class org.apache.spark.mllib.linalg.distributed.netCDFTest --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=$SCRATCH/spark/spark_event_logs ./tests/read_tests-app.py /global/cscratch1/sd/jialin/dayabay/dayabay-slice.csv 1 1000 /global/cscratch1/sd/jialin/dayabay/dayabay-final.h5 autoencoded 2759895880 


###check history server information####

#module load spark/hist-server
#./run_history_server.sh $EVENT_LOGS_DIR 

stop-all.sh
stop-collectl.sh
