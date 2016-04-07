import os,sys
import h5py,csv
from pyspark import SparkContext
import read

#read single large hdf5 file
def test_h5sparkReadsingle():
    sc=SparkContext(appName="h5sparkread_python_singlefile")
    rdd=read.h5read(sc,('/global/cscratch1/sd/jialin/climate/oceanTemps.hdf5','temperatures'),mode='single_chunked',partitions=3000) 
    rdd.cache()
    print "rdd count:",rdd.count()
    sc.stop()

if __name__ == '__main__':
    test_h5sparkReadsingle()
