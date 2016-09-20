#!/bin/bash


#SBATCH -p debug
#SBATCH -N 5 
#SBATCH -t 00:05:00
#SBATCH -e mysparkjob_%j.err
#SBATCH -o mysparkjob_%j.out
#module unload spark/hist-server
module load spark
start-all.sh

export LD_LIBRARY_PATH=$LD_LBRARY_PATH:$PWD/lib

###load single large hdf5 file####
repartition="200"
#inputfile="/scratch1/scratchdirs/jialin/celestial/dr12/photoObj-000109-1-0023.fits.h5"
inputfile="/global/cscratch1/sd/jialin/udf/bx_22860.h5"
#inputfile="/global/cscratch1/sd/jialin/io-ticket/udf-dbin/fake-2d-tiny.h5p"
app_name="H5Sspark-udf"
dataset="bx"

#spark-submit #--verbose\
#  --master $SPARKURL\
#  --name $app_name \

spark-submit --verbose\
  --master $SPARKURL\
  --driver-memory 100G\
  --executor-cores 32\
  --driver-cores 32\
  --num-executors=4\
  --conf spark.default.parallelism=$repartition\
  --executor-memory 100G\
  --class org.nersc.io.readtest\
  --conf spark.eventLog.enabled=true\
  --conf spark.eventLog.dir=$SCRATCH/spark/spark_event_logs\
  target/scala-2.10/h5spark-assembly-1.0.jar\
  $repartition $inputfile $dataset 

stop-all.sh
