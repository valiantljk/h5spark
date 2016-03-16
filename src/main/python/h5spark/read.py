
# coding: utf-8

# In[36]:

import h5py
from pyspark.mllib.linalg.distributed import  IndexedRowMatrix


# In[30]:

def readH5(sc, file_list_or_txt_file, partitions=None):
    '''Takes in a list of (file, dataset) tuples, one such tuple or the name of a file that contains
    a list of files and returns rdd with each row as a record'''

    #a list of tuples each with the pair (file_path, datasetname)
    if isinstance(file_list_or_txt_file,list):
        rdd = sc.parallelize(file_list_or_txt_file)
    
    #a string describing a file with list of hdf5 files
    elif isinstance(file_list_or_txt_file,str):
        rdd = sc.textFile(file_list_or_txt_file).map(lambda line: tuple(str(line).replace(" ", "").split(',')))
    elif isinstance(file_list_or_txt_file,tuple):
        rdd = sc.parallelize([file_list_or_txt_file])
    if partitions:
        rdd = rdd.repartition(partitions)
    ret = rdd.flatMap(readones)
    return ret

def h5ToIndexedRowMatrix(sc, file_list_or_txt_file, partitions=None):
    rdd = readH5(sc, file_list_or_txt_file, partitions=None)
    indexed_rows = rdd.zipWithIndex().map(lambda (v,k): (k,v))
    return IndexedRowMatrix(indexed_rows)
    
    
def readmul(paralist):
    x=[x.strip() for x in paralist.split(',')]
    return readones((x[0],x[1]))

#read one dataset/file each time. 
def readones(filename_dataname_tuple):
     filename, dataname = filename_dataname_tuple
     try:
        f=h5py.File(filename,'r')
        d=f[dataname]
        a=list(d[:])
     except Exception, e:
        print "ioerror:%s"%e, filename
     else:
          f.close()
          return a

#read a slice from one dataset/file
def readonep(paralist):
    x=[x.strip() for x in paralist.split(',')]
    try:
        f=h5py.File(x[0],'r')
        d=f[x[1]][int(x[2]):int(x[3]), : ]
        return list(d[:])
    except Exception, e:
           print "ioerror:%s"%e, x[0]
    finally:
        pass
        f.close()


# In[ ]:



