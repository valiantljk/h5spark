#!/bin/bash
#SBATCH -p regular 
#SBATCH -N 100
#SBATCH -t 00:10:00
#SBATCH -J h5read
#SBATCH -e test-col%j.err
#SBATCH -o test-col%j.out
#SBATCH --qos=premium
##arguments: 
## -i: inputfilename, -o:outputfilename, -b: collective_buffersize, -n: collective_buffernodes, -k: iscollective, -v: datasetname

##Test local 1.h5, change "SBATCH -N 25" to "SBATCH -N 1"
##srun -n 10 ./h5read -f 1.h5 -b 16777216 -n 2 -k 1  -v inputs 

##Test Dayabay data, one.h5 1.6 TB
#module load cray-hdf5-parallel
#srun -n 1600 ./h5read -f /global/homes/j/jialin/spark-io/h5spark/mpiio/9999.h5 -k 1 -v charge
#srun -n 1600 ./h5read -f /global/cscratch1/sd/jialin/dayabay/ost24/one.h5  -b 16777216 -n 50 -k 1 -v charge
#srun -n 1600 ./h5read -f /global/cscratch1/sd/jialin/dayabay/ost144/oceanTemps.hdf5  -k 1 -v temperatures 
srun -n 1600 ./h5read -f /global/cscratch1/sd/jialin/dayabay/dayabay-final.h5 -k 1 -v autoencoded
#srun -n 1600 ./h5read -f /global/cscratch1/sd/jialin/dayabay/ost24/oceanTemps.hdf5 -k 0 -v temperatures

#srun -n 1200 ./h5move -i /global/cscratch1/sd/gittens/CFSROhdf5/oceanTemps.hdf5 -o /global/cscratch1/sd/jialin/climate/temp2.h5 -b 16777216 -n 50 -k 0 -v temperatures


#srun -n 3200 ./h5write -f /scratch1/scratchdirs/jialin/hdf-data/test1.h5 -b 16777216 -n 40 -k 1 -x 3200000 -y 100000 
