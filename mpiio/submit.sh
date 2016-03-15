#!/bin/bash
#SBATCH -p debug 
#SBATCH -N 1
#SBATCH -t 00:10:00
#SBATCH -J h5read
#SBATCH -e %j.err
#SBATCH -o %j.out
#SBATCH -V

#input args: f: inputfilename, b: collective_buffersize, n: collective_buffernodes, k:iscollective, d:numberDIMs, v:datasetname
srun -n 10 ./h5read -f 1.h5 -b 16777216 -n 2 -k 1 -d 2 -v inputs 
