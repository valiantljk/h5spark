#!/bin/bash
#SBATCH -N 3
#SBATCH -t 00:20:00
#SBATCH -J h5write
#SBATCH -e %j.err
#SBATCH -o %j.out
#SBATCH -p debug
#SBATCH -C knl,quad,flat  
##SBATCH -A mpccc

##arguments: 
## -i: inputfilename, -o:outputfilename, -b: collective_buffersize, -n: collective_buffernodes, -k: iscollective, -v: datasetname


nodes=3
nprocs=2176 #32*68
#nprocs=1024
cbn=3
cbs=16777216
iscollective=1
dimx=2176000
#dimy=300000
dimy=150000
hostpartion=knl
rw=write
sbcast --compress=lz4 ./h5write /tmp/h5write
filename=$SCRATCH/hdf-data/ost248/test_${rw}_${hostpartion}_${nprocs}_${SLURM_JOBID}.h5
cmd="srun -n $nprocs  ./h5write -f $filename -b $cbs -n $cbn -k $iscollective -x $dimx -y $dimy"
echo $hostpartion
echo $cmd
$cmd
