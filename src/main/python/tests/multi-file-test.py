import os,sys
import h5py,csv
from pyspark import SparkContext
lib_path=os.path.abspath(os.path.join('..','h5spark'))
sys.path.append(lib_path)
import read

# read multiple hdf5 files
def test_h5sparkReadmultiple():
    if(len(sys.argv)!=4):
      print "arguments: csv_file_path number_partitions_csv number_partitions_hdf5"
      print len(sys.argv)
      sys.exit(1)
    csvfile=sys.argv[1]
    print "input csv file:",csvfile
    partitions=int(sys.argv[2])
    hdfpartitions=int(sys.argv[3])
    sc=SparkContext(appName="h5sparkread")
    file_paths = sc.textFile(csvfile, minPartitions=partitions)
    print "The number of files is %i" % file_paths.count()
    print "The number of partitions in file_paths %d"% file_paths.getNumPartitions()
    refile_paths=file_paths.repartition(hdfpartitions)
    print "Afte repartition: The number of partitions in file_paths %d"% refile_paths.getNumPartitions()
    rdd = refile_paths.flatMap(read.readmul)
    rdd.cache()
    print "1st time count: The number of elements in this rdd is %i" % rdd.count()
    print "2nd time count: The number of elements in this rdd is %i" % rdd.count()
    #the 2nd count will priorly ask spark to execute the rdd.cache()
    sc.stop()

if __name__ == '__main__':
    test_h5sparkReadmultiple()
