#!/bin/bash
#SBATCH -p debug 
#SBATCH -N 32
#SBATCH -t 00:20:00
#SBATCH -J h5write
#SBATCH -e %j.err
#SBATCH -o %j.out
#SBATCH -C haswell
#SBATCH -A mpccc
##arguments: 
## -i: inputfilename, -o:outputfilename, -b: collective_buffersize, -n: collective_buffernodes, -k: iscollective, -v: datasetname
 
nodes=32
nprocs=1024
cbn=32
cbs=16777216
iscollective=1
dimx=217600
dimy=300000
hostpartion=haswell
rw=write
sbcast --compress=lz4 ./h5write /tmp/h5write
filename=$SCRATCH/hdf-data/ost248/test_${rw}_${hostpartion}_${nprocs}_${SLURM_JOBID}.h5
cmd="srun -n $nprocs  ./h5write -f $filename -b $cbs -n $cbn -k $iscollective -x $dimx -y $dimy"
echo $hostpartion
echo $cmd
$cmd
