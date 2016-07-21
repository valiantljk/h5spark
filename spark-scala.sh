#!/bin/bash


#SBATCH -p debug
#SBATCH -N 2
#SBATCH -t 00:05:00
#SBATCH -e mysparkjob_%j.err
#SBATCH -o mysparkjob_%j.out
#SBATCH --ccm
##SBATCH --qos=premium
#module unload spark/hist-server
module load spark
#module load collectl
#start-collectl.sh 
start-all.sh

export LD_LIBRARY_PATH=$LD_LBRARY_PATH:$PWD/lib

###load single large hdf5 file####
repartition="200"
inputfile="/scratch1/scratchdirs/jialin/celestial/dr12/photoObj-000109-1-0023.fits.h5"
#inputfile="/global/cscratch1/sd/jialin/io-ticket/udf-dbin/fake-2d-tiny.h5p"
app_name="H5Sspark-udf"
dataset="HDU1/DATA/UERR"

spark-submit --verbose\
  --master $SPARKURL\
  --name $app_name \
  --driver-memory 50G\
  --executor-cores 24 \
  --driver-cores 24  \
  --num-executors=1 \
  --executor-memory 50G\
  --class org.nersc.io.readtest\
  --conf spark.eventLog.enabled=true\
  --conf spark.eventLog.dir=$SCRATCH/spark/spark_event_logs\
  target/scala-2.10/h5spark-assembly-1.0.jar \
  $repartition $inputfile $dataset 


rm /global/cscratch1/sd/jialin/spark_tmp_dir/*
stop-all.sh
#stop-collectl.sh
