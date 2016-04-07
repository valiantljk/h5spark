#!/bin/bash


#SBATCH -p regular
#SBATCH -N 46
#SBATCH -t 00:08:00
#SBATCH -e mysparkjob_%j.err
#SBATCH -o mysparkjob_%j.out
#SBATCH --ccm
#SBATCH --qos=premium
##SBATCH --volume="/global/cscratch1/sd/jialin/spark_tmp_dir/climate:/tmp:perNodeCache=size=200G"
module unload spark/hist-server
module load spark
module unload python
module load python/2.7-anaconda
module load collectl
start-collectl.sh 
start-all.sh

####load multiple hdf5 files###
export SPARK_LOCAL_DIRS="/tmp"
export PYTHONPATH=$PYTHONPATH:$PWD/src/main/python/h5spark

###load single large hdf5 file####
#inputfile="/global/cscratch1/sd/gittens/CFSROhdf5/oceanTemps.hdf5"
#inputfile="/global/cscratch1/sd/jialin/climate/oceanTemps.hdf5"
#dataset="temperatures"
#rows="6349676"
#type="64"

#inputfile="/global/cscratch1/sd/jialin/dayabay/ost1/dayabay-final.h5"
#inputfile="/global/cscratch1/sd/jialin/dayabay/dayabay-final.h5"
#dataset="autoencoded"
#rows="2759895880"
#type="32"
#csvlist="src/resources/hdf5/dayabay-slice.csv"

spark-submit --verbose \
 --master $SPARKURL \
 --executor-cores 32 \
 --driver-cores 32  \
 --num-executors=45  \
 --driver-memory 100G \
 --executor-memory 105G \
 --conf spark.eventLog.enabled=true \
 --conf spark.eventLog.dir=$SCRATCH/spark/spark_event_logs \
 src/main/python/tests/single-file-test.py \ 

##check history server information####


#export EVENT_LOGS_DIR=$SCRATCH/spark/spark_event_logs
#module load spark/hist-server
#run_history_server.sh $EVENT_LOGS_DIR 

stop-all.sh
stop-collectl.sh
