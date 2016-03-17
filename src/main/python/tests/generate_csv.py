import os,sys
import csv

def generate_csv(inputpath,dataset,outputcsv,lengthx, partition):
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
        if(len(sys.argv)!=6):
	  print "Usage: fileabpath,dataset, outcsv,numrows, stepsize"
	  sys.exit(1)
	generate_csv(sys.argv[1],sys.argv[2],sys.argv[3],int(sys.argv[4]), int(sys.argv[5]))



