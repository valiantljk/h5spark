import h5py
import sys
from pyspark import SparkContext

if __name__ == "__main__":
  """
  Usage: doit [partitions]
  """
  sc = SparkContext(appName="SparkHDF5")
  partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2

 # read a dataset and return it as a Python list #

 def f(x):
     a = x.split(",")
     with h5py.File(a[0]) as f:
         result = f[a[1]]
         return list(result[:])


 file_paths = sc.textFile("file_names_and_paths.csv", minPartitions=partitions)

 rdd = file_paths.flatMap(f)

 print "\ncount %d : min %f : mean %f : stdev %f : \
     max %f\n" % (rdd.count(), rdd.min(), rdd.mean(), \
                           rdd.stdev(), rdd.max())

 print rdd.histogram(10)
 sc.stop()