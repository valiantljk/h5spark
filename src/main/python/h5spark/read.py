
# coding: utf-8

# In[36]:

import h5py
from pyspark.mllib.linalg.distributed import  IndexedRowMatrix
import time
import os

# In[30]:

def h5read(sc,file_list_or_txt_file,mode='multi', partitions=None):
	if mode == 'multi':
		return readH5Multi(sc, file_list_or_txt_file, partitions)
	elif mode == 'single':
		return readH5SingleChunked(sc, file_list_or_txt_file, partitions)
	else:
		raise NotImplementedError("You specified a mode that is not implemented. Currently support multi small files (e.g., ~5G) parallel read mode: multi and single large file(TB) parallel read mode: single")

def readH5Multi(sc, file_list_or_txt_file, partitions=None):
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
    
    ret = rdd.map(lambda (f_path,dataset): (os.path.abspath(os.path.expandvars(os.path.expanduser(f_path))), dataset)).flatMap(readones)
    return ret

def readH5SingleChunked(sc, filename_dataset_tuple, partitions):
	assert isinstance(filename_dataset_tuple,tuple), "For single file mode, you must input a tuple."
	filename, dataset = filename_dataset_tuple
	rows = h5py.File(filename)[dataset].shape[0]
	if not partitions:
		partitions = rows / 50
	if partitions > rows:
		partitions = rows
	step = rows / partitions
	rdd = sc.range(0, rows, step)\
		.sortBy(lambda x: x, numPartitions=partitions)\
		.flatMap(lambda x: readonep(filename,dataset,x,step))
	return rdd

def h5read_irow(sc,file_list_or_txt_file, mode='multi', partitions=None):
    rdd = h5read(sc, file_list_or_txt_file,mode, partitions)
    indexed_rows = rdd.zipWithIndex().map(lambda (v,k): (k,v))
    return indexed_rows
	
def h5read_imat(sc, file_list_or_txt_file, mode='multi', partitions=None):
    rdd = h5read(sc, file_list_or_txt_file,mode, partitions)
    indexed_rows = rdd.zipWithIndex().map(lambda (v,k): (k,v))
    return IndexedRowMatrix(indexed_rows)

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
def readonep(filename, dset_name, i1, chunk_size):
	try:
		f=h5py.File(filename,'r')
		d = f[dset_name]
		if i1 + chunk_size < d.shape[0]:
			chunk = d[i1:i1+chunk_size,:]
		else:
			chunk = d[i1:d.shape[0],:]
		return list(chunk[:])
	except Exception, e:
		print "ioerror:%s"%e, filename
	finally:
		pass
		f.close()

# In[ ]:
