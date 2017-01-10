#!/bin/bash
##SBATCH -p debug 
#SBATCH -N 1
#SBATCH -t 00:10:00
#SBATCH -J h5write
#SBATCH -e %j.err
#SBATCH -o %j.out
srun -n 32 ./h5write -f $SCRATCH/hdf-data/test-darshan.h5 -b 16777216 -n 10 -k 1 -x 32 -y 300000
