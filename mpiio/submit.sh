#!/bin/bash
#SBATCH -p debug 
#SBATCH -N 25
#SBATCH -t 00:20:20
#SBATCH -J h5read
#SBATCH -e %j.err
#SBATCH -o %j.out

#input args: f: inputfilename, b: collective_buffersize, n: collective_buffernodes, k:iscollective, d:numberDIMs, v:datasetname

#Test local 1.h5, change "SBATCH -N 25" to "SBATCH -N 1"
#srun -n 10 ./h5read -f 1.h5 -b 16777216 -n 2 -k 1  -v inputs 

#Test Alex's 2TB
srun -n 800 ./h5read -f /global/cscratch1/sd/gittens/CFSROhdf5/oceanTemps.hdf5 -n 25 -k 1 -v temperatures
