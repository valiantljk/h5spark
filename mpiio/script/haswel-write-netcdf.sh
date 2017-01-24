#!/bin/bash
#SBATCH -N 1
#SBATCH -t 00:01:00
#SBATCH -J h5write
#SBATCH -e %j.err
#SBATCH -o %j.out
#SBATCH -p debug
#SBATCH -C haswell  
#SBATCH -A mpccc

srun -n 1 ./simple_xy_wr
