import os,sys
import h5py,csv
from pyspark import SparkContext
import read

#read single large hdf5 file
def test_h5sparkReadsingle():
    if(len(sys.argv)!=7):
         print "arguments: csv_file_path number_partitions_csv number_partitions_hdf5 input_file_path dataset maxdim"
	 print len(sys.argv)
         sys.exit(1)
    csvfile=sys.argv[1]
    print "input csv file:",csvfile
    partitions=int(sys.argv[2])
    hdfpartitions=int(sys.argv[3])
    input_file=sys.argv[4]
    dataset=sys.argv[5]
    maxdim=int(sys.argv[6])
    print "input_file_path",input_file
    print "dataset",dataset
    print "maxdim", maxdim
    #generate_csv(input_file,dataset,csvfile, maxdim,hdfpartitions)
    sc=SparkContext(appName="h5sparkread")
    file_paths = sc.textFile(csvfile, minPartitions=partitions)
    print "The number of files is %i" % file_paths.count()
    print "The number of partitions in file_paths %d"% file_paths.getNumPartitions()
    refile_paths=file_paths.repartition(hdfpartitions)
    print "Afte repartition: The number of partitions in file_paths %d"% refile_paths.getNumPartitions()
    ##count startup time

    rdd = refile_paths.flatMap(read.readonep)
    #rdd.cache()
    #print "1st time count: The number of elements in this rdd is %i" % rdd.count()
    #print "2nd time count: The number of elements in this rdd is %i" % rdd.count()
    rdd.first()
    sc.stop()
def generate_csv(inputpath,dataset,maxdim,outputcsv,partition):
    os.remove(outputcsv)
    lengthx=maxdim
        #2759895000
        #2759895880
    step=lengthx/partition #100000/100=100, 0:100
    with open(outputcsv,'wb') as csvfile:
          slicewriter=csv.writer(csvfile)
          next=0
          for i in range(0,partition-1):
                 slicewriter.writerow((inputpath,dataset,next,next+step))
                 next=next+step
                 #write remaining slice lenght as last row
                 slicewriter.writerow((inputpath,dataset,next,lengthx))
if __name__ == '__main__':
    test_h5sparkReadsingle()
