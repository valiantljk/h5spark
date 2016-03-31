/*
hdf5 reader in scala
*/
/************************************************************
  This example shows how to read and write data to a
  dataset by filename/datasetname.  The program first writes integers
  in a hyperslab selection to a dataset with dataspace
  dimensions of DIM_XxDIM_Y, then closes the file.  Next, it
  reopens the file, reads back the data, and outputs it to
  the screen.  Finally it reads the data again using a
  different hyperslab selection, and outputs the result to
  the screen.
 ************************************************************/
package org.nersc.io

import ncsa.hdf.hdf5lib._
import ncsa.hdf.hdf5lib.H5._
import ncsa.hdf.hdf5lib.HDF5Constants._
import ncsa.hdf.hdf5lib.exceptions.HDF5LibraryException
import ncsa.hdf.hdf5lib.exceptions.HDF5Exception
import org.slf4j.LoggerFactory
import scala.io.Source
import java.io.File

import org.apache.spark.mllib.linalg.distributed.DistributedMatrix._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext
object read {

  /**
   * Gets an NDimensional array of from a hdf
   * ***** @param FILENAME where the hdf file is located
   * *****@param DATASETNAME the hdf variable to search for
   * @param x:string contains (filename, datasetname), a line in a csv file
   * @return
   */

    def readone(x:String): (Array[Array[Float]])= {
    	var para =x.split(",")
    	var FILENAME = para{0}.trim
    	var DATASETNAME:String = para{1}.trim
    	var DIM_X: Int = 1
    	var DIM_Y: Int = 1
    	var RANK: Int = 1
    	var logger = LoggerFactory.getLogger(getClass)
    	var file_id = -2
    	var dataset_id = -2
	var dataspace_id = -2 
    	var dset_dims = new Array[Long](2)
    	dset_dims =Array(1,1)

	//Open an existing file
	try{
      	 file_id = H5Fopen(FILENAME, H5F_ACC_RDONLY, H5P_DEFAULT)
    	}
    	catch{
     	 case e: Exception=>  println("\nFile open error,filename:" + FILENAME+",file_id: "+file_id)
    	}

    	if (file_id < 0) {
	 logger.info("\nFile open error" + FILENAME)
    	}
   	
	//Open an existing dataset/variable
    	try{
    	 dataset_id = H5Dopen(file_id,DATASETNAME, H5P_DEFAULT)
    	}
    	catch{
	 case e: Exception=> println("\nDataset open error:" + DATASETNAME+"\nDataset_id: "+dataset_id)	 
    	}
    	if (dataset_id < 0) logger.info("File open error:" + FILENAME)
    
    	//Get dimension information of the dataset
    	try{
    	 dataspace_id =  H5Dget_space(dataset_id)
    	 H5Sget_simple_extent_dims(dataspace_id, dset_dims,null)
    	}
    	catch{
    	 case e: Exception=>println("Dataspace open error,dataspace_id: "+dataspace_id)
    	}
   	var dset_data = Array.ofDim[Float](dset_dims(0).toInt,dset_dims(1).toInt)    
    	// Read the data using the default properties.
    	var dread_id = -1
    	if (dataset_id >= 0){
       	  dread_id = H5Dread(dataset_id, H5T_NATIVE_FLOAT,
          H5S_ALL, H5S_ALL,
          H5P_DEFAULT, dset_data)
    	}
   	if(dread_id<0)
     	  logger.info("Dataset open error" + FILENAME)
  
    	dset_data
    }


    //loading 5 varialbes, each has 8 time step, then connect same timestep of variable into one vector
    //finally, return 8 by (5*) 2D double array, note the orginal data is float
    def readone_multiVariable(x:String): (Array[Array[Float]])= {
        var para =x.split(",")
        var FILENAME = para{0}.trim
        var DATASETNAME:String = para{1}.trim
        var DIM_X: Int = 1
        var DIM_Y: Int = 1
        var RANK: Int = 1
        var logger = LoggerFactory.getLogger(getClass)
        var file_id = -2
        var dataset_id = -2
        var dataspace_id = -2
        var dset_dims = new Array[Long](2)
        dset_dims =Array(1,1)

        //Open an existing file
        try{
         file_id = H5Fopen(FILENAME, H5F_ACC_RDONLY, H5P_DEFAULT)
        }
        catch{
         case e: Exception=>  println("\nFile open error,filename:" + FILENAME+",file_id: "+file_id)
        }

        if (file_id < 0) {
         logger.info("\nFile open error" + FILENAME)
        }

        //Open an existing dataset/variable
        try{
         dataset_id = H5Dopen(file_id,DATASETNAME, H5P_DEFAULT)
        }
        catch{
         case e: Exception=> println("\nDataset open error:" + DATASETNAME+"\nDataset_id: "+dataset_id)
        }
        if (dataset_id < 0) logger.info("File open error:" + FILENAME)

        //Get dimension information of the dataset
        try{
         dataspace_id =  H5Dget_space(dataset_id)
         H5Sget_simple_extent_dims(dataspace_id, dset_dims,null)
        }
        catch{
         case e: Exception=>println("Dataspace open error,dataspace_id: "+dataspace_id)
        }
        var dset_data = Array.ofDim[Float](dset_dims(0).toInt,dset_dims(1).toInt)
        // Read the data using the default properties.
        var dread_id = -1
        if (dataset_id >= 0){
          dread_id = H5Dread(dataset_id, H5T_NATIVE_FLOAT,
          H5S_ALL, H5S_ALL,
          H5P_DEFAULT, dset_data)
        }
        if(dread_id<0)
          logger.info("Dataset open error" + FILENAME)

        dset_data
    } 
   /* Currently, the function 'readonep' takes a string that contains "filename,variablename,startrowId,endrowId" and assumes the data is 2D double. 
   *  The function leverages the HDF5 JNI interface to read a hyperslab/chunk from the HDF5 file. 
   */ 
    def readonep(x:String): (Vector[Vector[Double]])= {
        var para =x.split(",")
        var FILENAME = para{0}.trim
        var DATASETNAME:String = para{1}.trim
        var start = para{2}.trim.toLong
        var end = para{3}.trim.toLong
        var DIM_X: Int = 1
        var DIM_Y: Int = 1
        var RANK: Int = 1
        var logger = LoggerFactory.getLogger(getClass)
        var file_id = -2
        var dataset_id = -2
        var dataspace_id = -2
	var ranks: Int = 2
        logger.info("\nStart: "+start+", End: "+end+"\n")

        //Open an existing file
        try{
         file_id = H5Fopen(FILENAME, H5F_ACC_RDONLY, H5P_DEFAULT)
        }
        catch{
         case e: Exception=>  logger.info("\nFile error: " + FILENAME)
        }
        if (file_id > 0)logger.info("\nFile ok\n")

        //Open an existing dataset/variable
        try{
         dataset_id = H5Dopen(file_id,DATASETNAME, H5P_DEFAULT)
        }
        catch{
         case e: Exception=> logger.info("\nDataset error\n")
        }
        if (dataset_id > 0) logger.info("\nDataset ok\n")


        //Get dimension information of the dataset
	var dset_dims=new Array[Long](2)
        try{
         dataspace_id =  H5Dget_space(dataset_id)
	 ranks=H5Sget_simple_extent_ndims(dataspace_id)
	 dset_dims = new Array[Long](ranks)
         H5Sget_simple_extent_dims(dataspace_id, dset_dims,null)
        }
        catch{
         case e: Exception=>logger.info("\nDataspace error")
        }
	if(dataspace_id>0) logger.info("\nDataspace ok\n")
	
	logger.info("\n"+dset_dims.mkString(" "))
        //var dset_data:Array[Array[Double]] = Array.ofDim[Double]((end-start).toInt,dset_dims(1).toInt)	
	//var dset_datas = Array.ofDim[Double]((end-start).toInt*dset_dims(1).toInt)

        var dset_data:Vector[Vector[Double]] = Vector.fill((end-start).toInt,dset_dims(1).toInt)(0.0)
        var dset_datas = Vector.fill((end-start).toInt*dset_dims(1).toInt)(0.0)

	var start_dims:Array[Long] = new Array[Long](ranks)
	var count_dims:Array[Long] = new Array[Long](ranks)
        start_dims(0) = start.toLong
	start_dims(1) = 0.toLong
	count_dims(0) = (end-start).toLong
	count_dims(1) = dset_dims(1).toLong
	logger.info(count_dims.mkString(" "))
	logger.info("\nMemory/Task "+count_dims(0)*count_dims(1)*8/1024.0/1024.0+" (MB)")

        var hyper_id = -2
	var dread_id = -2
	var memspace = -2
	H5Sclose(dataspace_id)

	//read data
        try{
	dataspace_id =  H5Dget_space(dataset_id)
	memspace = H5Screate_simple(ranks, count_dims,null)
	hyper_id = H5Sselect_hyperslab(dataspace_id, H5S_SELECT_SET,start_dims, null , count_dims, null)
	dread_id = H5Dread(dataset_id, H5T_NATIVE_DOUBLE,memspace, dataspace_id, H5P_DEFAULT, dset_data)
        }
	catch{
	  case e: java.lang.NullPointerException=>logger.info("data object is null")
	  case e@ ( _: HDF5LibraryException | _: HDF5Exception) =>logger.info("Error from HDF5 library|Failure in the data conversion. Read error info: "+e.getMessage+e.printStackTrace)
	}

	if(dread_id>0) logger.info("\nData read ok\n")
	/*
        var id=0
	var jd=0	
	for( id <-0 to (end-start).toInt-1){
	 for( jd <- 0 to (dset_dims(1)).toInt-1){
		dset_data(id)(jd)=dset_datas(id*((dset_dims(1))).toInt+jd)
		
	 }
	}	
	*/        
        dset_data
  }
}

def readH5SingleChunked(sc: SparkContext, filename: String, dataset: String, partitions: Long, rows: Long): RDD = {
	var num_partitions: Long = partitions
	if (rows > num_partitions) {
		num_partitions = rows
	}
	val step: Long = rows / num_partitions
	val rdd = sc.range(0, rows, step, partitions).flatMap(r  => readonep(filename, dataset, r, step)
	rdd
	

}

def h5ToIndexedRowMatrix(sc: SparkContext, filename: String, dataset: String, mode: String, partitions: Long): IndexedRowMatrix = {
	val rdd = readH5SingleChunked(sc, filename, dataset, partitions)
	val indexed_rows = rdd.zipWithIndex().map( k  => (k._1, k._2)).map(k => new IndexedRow(k._1, k._2))
	val IRM = new IndexedRowMatrix(indexed_rows)
	IRM

}

