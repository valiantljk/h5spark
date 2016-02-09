import h5py
import sys



# def __init__(self,filename,datasetname, cooridnates, conditions):
#     self.fname=filename
#     self.dname=datasetname
#     self.cord=cooridnates
#     self.cond=conditions
#read by coordinates, e.g., start:end=0:100

def readmul(paralist):
    x=[x.strip() for x in paralist.split(',')]
    #check file size,
        # if small, sequential read
        # if large, parallel read
        # on default, sequential read
    return readones(x[0],x[1])

def readones(filename,dataname):
    f=h5py.File(filename,'r')
    d=f[dataname]
    return list(d[:])

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
#def rangesplit(x):

#read by conditions, val<10
#def readbycond(self):
#    f=h5py.File(self.fname,'r')
#    d=f[self.dname]
#    return list(d[:])

