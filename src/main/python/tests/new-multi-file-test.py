
# coding: utf-8

# In[1]:

import os,sys
import h5py,csv
from pyspark import SparkContext
#lib_path=os.path.abspath(os.path.join('.','src/main/python/h5spark'))
#sys.path.append(lib_path)
from h5spark import read

def test_multi_2(sc,path_to_repo):
    
    resource_dir = 'src/resources/hdf5/'
    path = os.path.join(path_to_repo, resource_dir)
    h5_list = [(path + '/test_mat2.h5', 'data'),(path + '/test_mat3.h5', 'data') ]
    h5_list_file = 'file_names_and_pathsd.csv'
    h5_tuple = (path + '/test_mat2.h5', 'data')
    rdd = read.readH5(sc, h5_list)
    print rdd.take(1)
    #rdd2 = read.readH5(sc, h5_list_file)
    #print rdd2.take(1)
    rdd3 = read.readH5(sc, h5_tuple)
    print rdd3.take(1)
    
    idx_row_m = read.h5ToIndexedRowMatrix(sc, h5_tuple )
    print idx.rows.take(1)
    
    
    
    


# In[ ]:

if __name__ == '__main__':
    sc=SparkContext(appName="h5sparkread")
    path_to_repo = sys.argv[1]
    test_multi_2(sc,path_to_repo)
    

