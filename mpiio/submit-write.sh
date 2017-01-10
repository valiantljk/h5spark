#!/bin/bash
##SBATCH -p debug 
#SBATCH -N 1
#SBATCH -t 00:10:00
#SBATCH -J h5write
#SBATCH -e %j.err
#SBATCH -o %j.out
#SBATCH -C haswell
##SBATCH -p regular_knl
##SBATCH -C knl,quad,flat  
##SBATCH --ntasks-per-core=4
#SBATCH -A mpccc
##arguments: 
## -i: inputfilename, -o:outputfilename, -b: collective_buffersize, -n: collective_buffernodes, -k: iscollective, -v: datasetname

##Test local 1.h5, change "SBATCH -N 25" to "SBATCH -N 1"
##srun -n 10 ./h5read -f 1.h5 -b 16777216 -n 2 -k 1  -v inputs 

##Test Alex's 2TB
#module load cray-hdf5-parallel
#srun -n 1600 ./h5read -f /global/cscratch1/sd/gittens/CFSROhdf5/oceanTemps.hdf5  -b 16777216 -n 50 -k 0 -v temperatures
#srun -n 1200 ./h5move -i /global/cscratch1/sd/gittens/CFSROhdf5/oceanTemps.hdf5 -o /global/cscratch1/sd/jialin/climate/temp2.h5 -b 16777216 -n 50 -k 0 -v temperatures
#scratch=/scratch3/scratchdirs/jialin/hdf-data/ost
#scratch=$SCRATCH
#i=2
#rm $scratch$i/*
#sbcast --compress=lz4 h5write /tmp/h5write
#/global/cscratch1/sd/jialin/hdf-data/ost248 
nodes=1
#nprocs=2176 #32*68
nprocs=32
cbn=1
cbs=16777216
iscollective=1
dimx=2176
dimy=3000
hostpartion=haswell
rw=write
#sbcast --compress=lz4 ./h5write /tmp/h5write
filename=$SCRATCH/hdf-data/ost24/test_${rw}_${hostpartion}_${nprocs}_${SLURM_JOBID}.h5
cmd="srun -n $nprocs ./h5write -f $filename -b $cbs -n $cbn -k $iscollective -x $dimx -y $dimy"
echo $hostpartion
echo $cmd
$cmd
srun -n 1 -C haswell mount -l | grep 'cscratch1' 
