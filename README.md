# H5spark, 2016
1. Supporting Hierarchical Data Format, HDF5/NetCDF4 and Rich Parallel I/O Interface in Spark
2. Optimizing I/O Performance on Cray Machine with Lustre Filesystems

# Input and RDD Format
1. For reading multiple files, Input is "A csv file that lists file path and variable name", e.g., src/resources/hdf5/scalafilelist
2. For reading single file, Input is "A csv file that lists file path, variable name, and start, offset", e.g., src/resources/hdf5/
3. Output is "A single RDD in which the element of RDD is one row in original file"

#Download and Compile H5Spark:
1. git pull https://github.com/valiantljk/h5spark.git
2. cd h5spark
3. sbt package
4. cp target/scala-2.10/h5spark_2.10-1.0.jar lib/
5. cp -r lib/ your_project_dir/ (if you already have a lib directory, then just copy everything in h5spark/lib/* to your lib/)

#Sample Batch Job Script on Cori
1. Python version: sbatch spark-python.sh 
2. Scala version: sbatch spark-scala.sh


#Use in Your Pyspark Scripts:
Add this to your python path:
	export PYTHONPATH= path/to/h5spark/src/main/python/:$PYTHONPATH

Then import it in python like so:

1. from h5spark import read
2. from pyspark import SparkContext
3. sc = SparkContext()
4. rdd = read.readH5(sc,('path/to/h5file', 'dataset_name'))

#Use H5spark in your Scala Codes
1. export LD_LIBRARY_PATH=$LD_LBRARY_PATH:your_project_dir/lib
2. add these lines in your codes:   import org.nersc.io._
3. then you have a few options to load the data
4. the inputpath can be an absolute path of a single large HDF5 file, can also be a path to multiple small HDF5 files, e.g, a directory that contains millions of files

** Load as an indexedmatrix: val tempmat = read.h5read_imat (sc,inputpath, variablename, partition)

** Load as an indexedrow: val tempmat = read.h5read_irow (sc,inputpath, variablename, partition)

** Load as an array: val tempmat = read.h5read (sc,inputpath, variablename, partition)


