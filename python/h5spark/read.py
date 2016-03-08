import h5py

def readmul(paralist):
    x=[x.strip() for x in paralist.split(',')]
    return readones(x[0],x[1])

#read one dataset/file each time. 
def readones(filename,dataname):
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