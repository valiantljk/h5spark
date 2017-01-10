#!/bin/bash
#SBATCH -p debug 
#SBATCH -N 10
#SBATCH -t 00:10:00
#SBATCH -J h5write
#SBATCH -e %j.err
#SBATCH -o %j.out
#DW jobdw capacity=212GB access_mode=striped type=scratch
#SBATCH -A mpccc

module load dws
mkdir -p $DW_JOB_STRIPED/hdf-data/
srun -n 240 ./h5write -f $DW_JOB_STRIPED/hdf-data/test4.h5 -b 16777216 -n 10 -k 1 -x 240000 -y 300000
