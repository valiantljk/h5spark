#!/bin/bash
#SBATCH -p regular 
#SBATCH -N 45
#SBATCH -t 00:03:00
#SBATCH -J h5read
#SBATCH -e h5spark-mpiio-ind%j.err
#SBATCH -o h5spark-mpiio-ind%j.out
#SBATCH --qos=premium
##arguments: 
## -i: inputfilename, -o:outputfilename, -b: collective_buffersize, -n: collective_buffernodes, -k: iscollective, -v: datasetname
srun -n 1440 ./h5read -f /global/cscratch1/sd/jialin/dayabay/ost248/oceanTemps.hdf5 -k 0 -v temperatures

