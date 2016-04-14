#![Alt text](https://cloud.githubusercontent.com/assets/1396867/14511488/a9bf8820-018c-11e6-9c11-f385f9f628f6.png)
1. Support Hierarchical Data Format, HDF5/NetCDF4 and Rich Parallel I/O Interface in Spark
2. Optimize I/O Performance on HPC with Lustre Filesystems Tuning

# Input and Output
1. Input is a HDF5 file
3. Output is a RDD object

#Download and Compile H5Spark
1. git pull https://github.com/valiantljk/h5spark.git
2. cd h5spark
3. module load sbt (if on NERSC's machine,otherwise, you need to install sbt first)
4. sbt package


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

object readtest {
 def main(args: Array[String]): Unit = {
    var logger = LoggerFactory.getLogger(getClass)
    var partition = args(0).toInt
    var input = args(1)
    var variable = args(2)
    val sc = new SparkContext()
    val rdd = read.h5read (sc,input,variable,partition)
    rdd.cache()
    val count= rdd.count()
    logger.info("\nRDD_Count: "+count+" , Total number of rows of all hdf5 files\n")
    sc.stop()
  }

}
```

You can also load the HDF5 data into other RDD formats, e.g., 
```
val rdd = read.h5read_vec (sc,inputpath, variablename, partition) //Load as an indexedvector: 
val rdd = read.h5read_irow (sc,inputpath, variablename, partition) //Load as an indexedrow: 
val rdd = read.h5read_imat (sc,inputpath, variablename, partition) //Load as an indexedmatrix: 
```

#Sample Batch Job Script on Cori
If you have an NERSC account(email consult@nersc.gov to get one), you can try with the batch scripts:

1. Python version: sbatch spark-python.sh
2. Scala version: sbatch spark-scala.sh

#Questions and Support
1. If you are using NERSC's machine, please feel free to email consult@nersc.gov 
2. If not, you can send your questions to jalnliu@lbl.gov

#Citation
J.L. Liu, E. Racah, Q. Koziol, R. S. Canon, A. Gittens, L. Gerhardt, S. Byna, M. F. Ringenburg, Prabhat. "H5Spark: Bridging the I/O Gap between Spark and Scientific Data Formats on HPC Systems", Cray User Group, 2016, (paper available in May 2016)
