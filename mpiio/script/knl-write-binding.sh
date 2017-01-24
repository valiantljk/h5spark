#!/bin/bash
#SBATCH -N 32
#SBATCH -t 00:10:00
#SBATCH -J h5write
#SBATCH -e %j.err
#SBATCH -o %j.out
#SBATCH -p regular_knl
#SBATCH -C knl,quad,flat  
#SBATCH -A mpccc
#SBATCH -S 4
##arguments: 
## -i: inputfilename, -o:outputfilename, -b: collective_buffersize, -n: collective_buffernodes, -k: iscollective, -v: datasetname


nodes=32
#nprocs=2176 #32*68
#nprocs=512
nprocs=1024
cbn=32
cbs=16777216
iscollective=1
dimx=217600
dimy=300000
hostpartion=knl
rw=write
sbcast --compress=lz4 ./h5write /tmp/h5write
filename=$SCRATCH/hdf-data/ost248/test_${rw}_${hostpartion}_${nprocs}_${SLURM_JOBID}.h5
#spc 68*4/32=8.5
cmd="srun -n $nprocs -c 8 --cpu_bind=cores ./h5write -f $filename -b $cbs -n $cbn -k $iscollective -x $dimx -y $dimy"
#numatcl -p 1
#echo "cpu spec is 4"
echo $hostpartion
echo $cmd
$cmd
