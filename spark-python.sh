#!/bin/bash


#SBATCH -p debug
#SBATCH -N 5 
#SBATCH -t 00:10:00
#SBATCH -e mysparkjob_%j.err
#SBATCH -o mysparkjob_%j.out
#SBATCH --ccm
#SBATCH --volume="/global/cscratch1/sd/jialin/spark_tmp_dir/climate:/tmp:perNodeCache=size=200G"
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
#spark-submit --master $SPARKURL --executor-cores 1 --driver-memory 20G --executor-memory 80G  --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=$SCRATCH/spark/spark_event_logs --py-files src/main/python/h5spark/read.py src/main/python/tests/multi-file-test.py  src/resources/hdf5/python-filelist-sample 1 10 

###load single large hdf5 file####
partition="1"
repartition="20"

#inputfile="/global/cscratch1/sd/gittens/CFSROhdf5/oceanTemps.hdf5"
inputfile="/global/cscratch1/sd/jialin/climate/oceanTemps.hdf5"
dataset="temperatures"
rows="6349676"
type="64"
#csvlist="src/resources/hdf5/oceanlist3k.csv"
#csvlist="src/resources/hdf5/oceanlist.csv"
csvlist="src/resources/hdf5/oceanlist20.csv"
#inputfile="/global/cscratch1/sd/jialin/dayabay/ost1/dayabay-final.h5"
#inputfile="/global/cscratch1/sd/jialin/dayabay/dayabay-final.h5"
#dataset="autoencoded"
#rows="2759895880"
#type="32"
#csvlist="src/resources/hdf5/dayabay-slice.csv"

spark-submit --verbose \
 --master $SPARKURL \
 --executor-cores 5 \
 --driver-cores 32  \
 --num-executors=4  \
 --driver-memory 100G \
 --executor-memory 100G \
 --conf spark.eventLog.enabled=true \
 --conf spark.eventLog.dir=$SCRATCH/spark/spark_event_logs \
 src/main/python/tests/single-file-test.py \
 $csvlist $partition $repartition $inputfile $dataset $rows 

#--conf spark.locality.wait=0 \
#arguments: csv_file_path number_partitions_csv number_partitions_hdf5 input_file_path dataset maxdim

#python generate_csv.py /global/cscratch1/sd/gittens/CFSROhdf5/oceanTemps.hdf5 temperatures ../../../resources/hdf5/oceanlist.csv 6349676 2000
#python generate_csv.py /global/cscratch1/sd/jialin/climate/oceanTemps.hdf5 temperatures ../../../resources/hdf5/oceanlist.csv 6349676 2000
#python generate_csv.py /global/cscratch1/sd/jialin/dayabay/ost1/dayabay-final.h5 autoencoded ../../../resources/hdf5/dayabay-slice.csv 2759895880 5000
# 2759895880 is the total number of rows in the 2d array, the number of columns is 11(reduced from 192)
###check history server information####


#export EVENT_LOGS_DIR=$SCRATCH/spark/spark_event_logs
#module load spark/hist-server
#run_history_server.sh $EVENT_LOGS_DIR 

stop-all.sh
stop-collectl.sh
