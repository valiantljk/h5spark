import os,sys
import csv

def generate_csv(inputpath,dataset,outputcsv,partition):
        #f=h5py.File(inputpath)
        lengthx=2759895880
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

if __name__== '__main__':
	generate_csv('/global/cscratch1/sd/jialin/dayabay/dayabay-final.h5','inputs','/global/cscratch1/sd/jialin/dayabay/slicelist.csv',1000)



