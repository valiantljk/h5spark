#!/bin/bash


#SBATCH -p regular
#SBATCH -N 5 
#SBATCH -t 00:2:00
#SBATCH -e mysparkjob_%j.err
#SBATCH -o mysparkjob_%j.out
#SBATCH --ccm
##SBATCH --reservation=INC0082872
#SBATCH --qos=premium
##SBATCH --volume="/global/cscratch1/sd/jialin/spark_tmp_dir/climate:/tmp:perNodeCache=size=200G"
module unload spark/hist-server
module load spark
module load collectl
start-collectl.sh 
start-all.sh

# to create a fat jar
# sbt assembly
# test the multiple hdf5 file reader:
#export SPARK_LOCAL_DIRS="/tmp"
export LD_LIBRARY_PATH=$LD_LBRARY_PATH:$PWD/lib

###load single large hdf5 file####
repartition="100"
#inputfile="/global/cscratch1/sd/jialin/climate/oceanTemps.hdf5"
#inputfile="/global/cscratch1/sd/jialin/dayabay/ost24/oceanTemps.hdf5"
inputfile="/global/cscratch1/sd/jialin/dayabay/2016/sci-h5spark/five.h5"
#app_name="dayabay/2016/data-singlef-scala/"
#app_name="dayabay/ost8/multi-medium-multif-scala/"
#app_name="ost24/oceanTemps.hdf5-h5spark"
#app_name="ost24/oceanTemps.hdf5-2000par-cache"
app_name="H5SsparkTest-five11"
#inputfile="/global/cscratch1/sd/jialin/dayabay/ost72/"
#inputfile="/global/cscratch1/sd/jialin/dayabay/ost8/multi-medium"
#inputfile = "/global/cscratch1/sd/gittens/large-climate-dataset/data/production/T.h5"
#inputfile="/global/cscratch1/sd/jialin/dayabay/dayabay-final.h5"
#dataset="temperatures"
dataset="charge"
#dataset="rows"
#dataset="autoencoded"
func="array"


spark-submit --verbose\
  --master $SPARKURL\
  --name $app_name \
  --driver-memory 100G\
  --executor-cores 32 \
  --driver-cores 32  \
  --num-executors=4 \
  --executor-memory 105G\
  --class org.nersc.io.readtest\
  --conf spark.eventLog.enabled=true\
  --conf spark.eventLog.dir=$SCRATCH/spark/spark_event_logs\
  target/scala-2.10/h5spark-assembly-1.0.jar \
  $repartition $inputfile $dataset 


#  $argsjava
#$csvlist $partition $repartition $inputfile $dataset $rows
# check history server information####
# module load spark/hist-server
# ./run_history_server.sh $EVENT_LOGS_DIR 
rm /global/cscratch1/sd/jialin/spark_tmp_dir/*
stop-all.sh
stop-collectl.sh
