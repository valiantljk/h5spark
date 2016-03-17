# H5spark, 2016
1. Supporting Hierarchical Data Format, HDF5/NetCDF4 and Rich Parallel I/O Interface in Spark
2. Optimizing I/O Performance on Cray Machine with Lustre Filesystems

# Input and Ouput
1. For reading multiple files, Input is "A csv file that lists file path and variable name", e.g., src/resources/hdf5/scalafilelist
2. For reading single file, Input is "A csv file that lists file path, variable name, and start, offset", e.g., src/resources/hdf5/
3. Output is "A single RDD in which the element of RDD is one row in original file"

#Run on Cori
1. Python version: sbatch spark-python.sh
2. Scala version: sbatch spark-scala.sh

#To Use in Your Pyspark Scripts:
Add this to your python path:

export PYTHONPATH= path/to/h5spark/src/main/python/:$PYTHONPATH

Then import it in python like so:
1. from h5spark import read
2. from pyspark import SparkContext
3. sc = SparkContext()
4. rdd = read.readH5(sc,('path/to/h5file', 'dataset_name'))
