#!/bin/bash
#SBATCH -p debug 
#SBATCH -N 50
#SBATCH -t 00:20:20
#SBATCH -J h5move
#SBATCH -e %j.err
#SBATCH -o %j.out

##arguments: 
## -i: inputfilename, -o:outputfilename, -b: collective_buffersize, -n: collective_buffernodes, -k: iscollective, -v: datasetname

##Test local 1.h5, change "SBATCH -N 25" to "SBATCH -N 1"
##srun -n 10 ./h5read -f 1.h5 -b 16777216 -n 2 -k 1  -v inputs 

##Test Alex's 2TB
module load cray-hdf5-parallel
srun -n 1600 ./h5move -i /global/cscratch1/sd/gittens/CFSROhdf5/oceanTemps.hdf5 -o /global/cscratch1/sd/jialin/hdf-data/climate/temp1.h5  -n 25 -k 0 -v temperatures
