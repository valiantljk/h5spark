#![Alt text](https://cloud.githubusercontent.com/assets/1396867/14511488/a9bf8820-018c-11e6-9c11-f385f9f628f6.png)
1. Support Hierarchical Data Format, HDF5/NetCDF4 and Rich Parallel I/O Interface in Spark
2. Optimize I/O Performance on HPC with Lustre Filesystems Tuning

# Input and Output
1. Input is a HDF5 file
3. Output is a RDD object

#Download H5Spark
1. git clone https://github.com/valiantljk/h5spark.git

#Simply Test H5spark on Cori/Edison
Python version:

1. export PYTHONPATH=$PYTHONPATH:path_to_h5spark/src/main/python/h5spark
2. sbatch spark-python.sh

Scala version:

1. export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:path_to_h5spark/lib
2. module load sbt
3. sbt assembly
4. sbatch spark-scala.sh

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

Current h5spark python read API:

Read single file: 
```
h5read(sc,(file,dataset),mode='single', partitions)
```

Read multiple files:

Takes in a list of (file, dataset) tuples, one such tuple or the name of a file that contains
    a list of files and returns rdd with each row as a record
```
h5read(sc,file_list_or_txt_file,mode='multi', partitions)

```

Besides, we have the functions to return indexedrow and indexedrowmatrix
```
h5read_irow
h5read_imat
```

#Use in Scala Codes
1. export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:your_project_dir/lib
2. cp h5spark/target/scala-2.10/h5spark_2.10-1.0.jar your_project_dir/lib/
3. cp h5spark/lib/* your_project_dir/lib/
4. cp project/assembly.sbt your_project_dir/project/
5. sbt assembly
6. Then in your codes, you can use it like:
```
import org.nersc.io._

object readtest {
 def main(args: Array[String]): Unit = {
    var logger = LoggerFactory.getLogger(getClass)
    val sc = new SparkContext()
    val rdd = read.h5read_array (sc,"oceanTemps.h5","temperatures", 3000)
    rdd.cache()
    val count= rdd.count()
    logger.info("\nRDD_Count: "+count+" , Total number of rows of all hdf5 files\n")
    sc.stop()
  }

}
```

Current h5spark scala read API supports:

```
val rdd = read.h5read_point (sc, inputpath, variablename, partition) //load n-D data into RDD[(value:Double,key:Long)]
val rdd = read.h5read_array (sc, inputpath, variablename, partition) //load n-D data into RDD[Array[Double]]
val rdd = read.h5read_vec (sc,inputpath, variablename, partition) //Load n-D data into RDD[DenseVector] 
val rdd = read.h5read_irow (sc,inputpath, variablename, partition) //Load n-D data into RDD[IndexedRow] 
val rdd = read.h5read_imat (sc,inputpath, variablename, partition) //Load n-D data into IndexedRowMatrix
```

#Questions and Support
1. If you are using NERSC's machine, please feel free to email consult@nersc.gov 
2. If not, you can send your questions to jalnliu@lbl.gov

#Citation
J.L. Liu, E. Racah, Q. Koziol, R. S. Canon, A. Gittens, L. Gerhardt, S. Byna, M. F. Ringenburg, Prabhat. "H5Spark: Bridging the I/O Gap between Spark and Scientific Data Formats on HPC Systems", Cray User Group, 2016, ([Paper](https://github.com/valiantljk/h5spark/files/261834/h5spark-cug16-final.pdf),
[Slides](https://github.com/valiantljk/h5spark/files/261837/h5spark-2016-cug.pdf),
[Bib](https://github.com/valiantljk/h5spark/files/261861/h5spark.bib.txt))

#Highlight
1. Tested at full scale on Cori phase 1, with 1600 nodes, 51200 cores. H5Spark took 2 minutes to load 16 TBs HDF5 2D data
2. H5Spark takes 35 seconds in loading 2 TB data, while MPI uses 15 seconds. 
