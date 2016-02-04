import h5py
from pyspark import SparkContext
import unittest
from h5spark import read
#from mpi4py import MPI

class h5pytestinspark(unittest.TestCase):
    def setUp(self):
        self.csvfile='../resources/hdf5/file_names_and_paths.csv'
        self.csvfiled='../resources/hdf5/file_names_and_pathsd.csv'
        self.partitions=1
        self.dname='autoencoded'
        self.testfpath="../resources/hdf5/16041.h5"
        self.testfname="16041.h5"
        self.sc=SparkContext(appName="h5sparkread")
    def tearDown(self):
        self.sc.stop()

    """ Test h5py """
    def test_h5pyreads(self):
        self.testfile = h5py.File(self.testfpath,'r')
        self.dset=self.testfile[self.dname]
        self.assertIn(self.testfname,self.testfile.filename)

    #def test_h5pyreadp(self):
        #rank = MPI.COMM_WORLD.rank
        #self.testfile = h5py.File(self.testfpath,'r', driver='mpio', comm=MPI.COMM_WORLD)
        #self.testfile.atomic=True
        #self.dset=self.testfile[self.dname]
        #self.assertIn(self.testfname,self.testfile.filename)


    """ Test h5spark"""
    # read one or more hdf5 files where the input file info is in a csv file
    def test_h5sparkReadmultiple(self):
        self.read=read.readmul
        self.file_paths = self.sc.textFile(self.csvfile, minPartitions=self.partitions)
        self.rdd = self.file_paths.flatMap(self.read)
        self.msg="number of elements not equal"
        self.assertEqual(200000, self.rdd.count(),self.msg)

    # read 1 small hdf5 file by file name
    def test_h5sparkReadones(self):
        self.read=read.readones
        self.rdd = self.sc.parallelize(self.read(self.testfpath, self.dname))
        self.msg="number of elements not equal"
        self.assertEqual(20000, self.rdd.count(),self.msg)


    # read 1 large hdf5 file by file name
    def test_h5sparkReadonep(self):
        self.read=read.readonep
        self.file_paths=self.sc.textFile(self.csvfiled,minPartitions=self.partitions)
        #self.range=read.rangesplit
        self.rdd=self.file_paths.flatMap(self.read)
        #self.rdd = self.sc.parallelize(self.read(self.testfpath, self.dname))
        self.msg="number of elements not equal %d" % self.rdd.count()
        self.assertEqual(20000, self.rdd.count(),self.msg)

if __name__ == '__main__':
    unittest.main()