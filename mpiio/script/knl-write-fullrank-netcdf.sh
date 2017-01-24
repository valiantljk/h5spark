#!/bin/bash
#SBATCH -N 1
#SBATCH -t 00:02:00
#SBATCH -J h5write
#SBATCH -e %j.err
#SBATCH -o %j.out
#SBATCH -p regular_knl
#SBATCH -C knl,quad,flat  
#SBATCH -A mpccc

srun -n 1 ./simple_xy_wr
