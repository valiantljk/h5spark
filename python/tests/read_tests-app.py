import os,sys
import h5py,csv
from pyspark import SparkContext
lib_path=os.path.abspath(os.path.join('..','tests','h5spark'))
sys.path.append(lib_path)
import read

def test_h5sparkReadmultiple():
	if(len(sys.argv)!=4):
	 print "arguments: csv_file_path number_partitions_csv number_partitions_hdf5"
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
        #print "The number of executers is %d" % sc.getExecutorStorageStatus().size()-1
	#rdd.persist()
	print "1st time count: The number of elements in this rdd is %i" % rdd.count()
	print "2nd time count: The number of elements in this rdd is %i" % rdd.count()
	#the 2nd count will priorly ask spark to execute the rdd.cache()
        #print "The first element in this rdd has %i numbers" % len(rdd.first())
        #print rdd.first()
	sc.stop()

def test_h5sparkReadsingle():
	if(len(sys.argv)!=7):
         print "arguments: csv_file_path number_partitions_csv number_partitions_hdf5 input_file_path dataset maxdim"
         sys.exit(1)
        csvfile=sys.argv[1]
        print "input csv file:",csvfile
        partitions=int(sys.argv[2])
        hdfpartitions=int(sys.argv[3])
	input_file=sys.argv[4]
	dataset=sys.argv[5]
	maxdim=int(sys.argv[6])
	generate_csv(input_file,dataset,maxdim,csvfile,hdfpartitions)
        sc=SparkContext(appName="h5sparkread")
        file_paths = sc.textFile(csvfile, minPartitions=partitions)
        print "The number of files is %i" % file_paths.count()
        print "The number of partitions in file_paths %d"% file_paths.getNumPartitions()
        refile_paths=file_paths.repartition(hdfpartitions)
        print "Afte repartition: The number of partitions in file_paths %d"% refile_paths.getNumPartitions()
        rdd = refile_paths.flatMap(read.readonep)
        rdd.cache()
        #print "The number of executers is %d" % sc.getExecutorStorageStatus().size()-1
        #rdd.persist()
        print "1st time count: The number of elements in this rdd is %i" % rdd.count()
        print "2nd time count: The number of elements in this rdd is %i" % rdd.count()
        #the 2nd count will priorly ask spark to execute the rdd.cache()
        #print "The first element in this rdd has %i numbers" % len(rdd.first())
        #print rdd.first()
        sc.stop()
def generate_csv(inputpath,dataset,maxdim,outputcsv,partition):
        #f=h5py.File(inputpath)
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
    #test_h5sparkReadmultiple()
    test_h5sparkReadsingle()
