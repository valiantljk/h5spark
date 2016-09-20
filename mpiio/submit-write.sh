#!/bin/bash
#SBATCH -p debug 
#SBATCH -N 32
#SBATCH -t 00:10:00
#SBATCH -J h5write
#SBATCH -e %j.err
#SBATCH -o %j.out
##arguments: 
## -i: inputfilename, -o:outputfilename, -b: collective_buffersize, -n: collective_buffernodes, -k: iscollective, -v: datasetname

##Test local 1.h5, change "SBATCH -N 25" to "SBATCH -N 1"
##srun -n 10 ./h5read -f 1.h5 -b 16777216 -n 2 -k 1  -v inputs 

##Test Alex's 2TB
#module load cray-hdf5-parallel
#srun -n 1600 ./h5read -f /global/cscratch1/sd/gittens/CFSROhdf5/oceanTemps.hdf5  -b 16777216 -n 50 -k 0 -v temperatures
#srun -n 1200 ./h5move -i /global/cscratch1/sd/gittens/CFSROhdf5/oceanTemps.hdf5 -o /global/cscratch1/sd/jialin/climate/temp2.h5 -b 16777216 -n 50 -k 0 -v temperatures
scratch=/scratch3/scratchdirs/jialin/hdf-data/ost
i=2
rm $scratch$i/*
srun -n 320 ./h5write -f $SCRATCH/hdf-data/test12.h5 -b 16777216 -n 10 -k 1 -x 24000 -y 300000
