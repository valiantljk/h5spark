#!/bin/bash
#SBATCH -p debug 
#SBATCH -N 1 
#SBATCH -t 00:10:00
#SBATCH -J h5write
#SBATCH -e %j.err
#SBATCH -o %j.out
#SBATCH -C haswell
##SBATCH -A mpccc
##arguments: 
## -i: inputfilename, -o:outputfilename, -b: collective_buffersize, -n: collective_buffernodes, -k: iscollective, -v: datasetname
  
nodes=32       #Nodes 32
nprocs=1024    #Processes 1024
cbn=32         #Aggregators 32
cbs=16777216   #Collective Buffer size 16MB
iscollective=1 #Collective IO
dimx=3200      #Size of X dimension
dimy=300000    #Size of Y dimension
lost=72        #OST 72
hostpartion=haswell
sbcast --compress=lz4 ./h5write /tmp/h5write
filename=$SCRATCH/hdf-data/ost${lost}/test_${hostpartion}_${nprocs}_${SLURM_JOBID}.h5
cmd="srun -n $nprocs  ./h5write -f $filename -b $cbs -n $cbn -k $iscollective -x $dimx -y $dimy"
echo $hostpartion
echo $cmd
$cmd
