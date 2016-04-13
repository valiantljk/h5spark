#![Alt text](/../master/src/resources/h5spark.png?raw=true )
1. Support Hierarchical Data Format, HDF5/NetCDF4 and Rich Parallel I/O Interface in Spark
2. Optimize I/O Performance on HPC with Lustre Filesystems Tuning

# Input and Output RDD Format
1. Input is a tuple of (pathname or filename, variablename, numpartitions)
3. Output is "A single RDD in which the element of RDD is one row in its original file(s)"

#Download and Compile H5Spark
1. git pull https://github.com/valiantljk/h5spark.git
2. cd h5spark
3. sbt package
4. cp target/scala-2.10/h5spark_2.10-1.0.jar lib/
5. cp -r lib/ your_project_dir/ (if you already have a lib directory, then just copy everything in h5spark/lib/* to your lib/)

#Use in Pyspark Scripts
Add this to your python path:
	export PYTHONPATH= path/to/h5spark/src/main/python/:$PYTHONPATH

Then import it in python like so:

1. from h5spark import read
2. from pyspark import SparkContext
3. sc = SparkContext()
4. rdd = h5read(sc,file_list_or_txt_file,mode='multi', partitions=2000)

#Use in Scala Codes
1. export LD_LIBRARY_PATH=$LD_LBRARY_PATH:your_project_dir/lib
2. add these lines in your codes:   import org.nersc.io._
3. then you have a few options to load the data
4. the inputpath can be an absolute path of a single large HDF5 file, can also be a path to multiple small HDF5 files, e.g, a directory that contains millions of files

** Load as an array: val rdd = read.h5read (sc,inputpath, variablename, partition)

** Load as an indexedvector: val rdd = read.h5read_vec (sc,inputpath, variablename, partition)

** Load as an indexedrow: val rdd = read.h5read_irow (sc,inputpath, variablename, partition)

** Load as an indexedmatrix: val rdd = read.h5read_imat (sc,inputpath, variablename, partition)



#Sample Batch Job Script for testing on Cori
1. Python version: sbatch spark-python.sh 
2. Scala version: sbatch spark-scala.sh
