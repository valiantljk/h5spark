#![Alt text](https://cloud.githubusercontent.com/assets/1396867/14511488/a9bf8820-018c-11e6-9c11-f385f9f628f6.png)
1. Support Hierarchical Data Format, HDF5/NetCDF4 and Rich Parallel I/O Interface in Spark
2. Optimize I/O Performance on HPC with Lustre Filesystems Tuning

# Input and Output
1. Input is a tuple of (pathname or filename, variablename, numpartitions)
3. Output is "A single RDD in which the element of RDD is one row in its original file(s)"

#Download and Compile H5Spark
1. git pull https://github.com/valiantljk/h5spark.git
2. cd h5spark
3. sbt package


#Use in Pyspark Scripts

Add the h5spark path to your python path:

export PYTHONPATH=$PYTHONPATH:path_to_h5spark/src/main/python/h5spark

Then your python codes will be like so:

```
from pyspark import SparkContext
import os,sys
import h5py
import read

def test_h5sparkReadsingle():
     sc = SparkContext(appName="h5sparktest")
     rdd=read.h5read(sc,('oceanTemps.h5','temperatures'),mode='single',partitions=100)
     rdd.cache()
     print "rdd count:",rdd.count()
     sc.stop()

if __name__ == '__main__':
    test_h5sparkReadsingle()
```
#Use in Scala Codes
1. export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:your_project_dir/lib
2. cp h5spark/target/scala-2.10/h5spark_2.10-1.0.jar your_project_dir/lib/
3. cp h5spark/lib/* your_project_dir/lib/
4. Then in your codes, you can use it like:
```
import org.nersc.io._

val rdd = read.h5read (sc,inputpath, variablename, partition)

//Or you can have other rdd formats:

val rdd = read.h5read_vec (sc,inputpath, variablename, partition) //Load as an indexedvector: 
val rdd = read.h5read_irow (sc,inputpath, variablename, partition) //Load as an indexedrow: 
val rdd = read.h5read_imat (sc,inputpath, variablename, partition) //Load as an indexedmatrix: 
```

#Sample Batch Job Script for testing on Cori
1. Python version: sbatch spark-python.sh 
2. Scala version: sbatch spark-scala.sh
