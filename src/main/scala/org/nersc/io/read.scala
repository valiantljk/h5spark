/*
hdf5 reader in scala
*/
/************************************************************
  This example shows how to read and write data to a
  dataset by hyberslabs.  The program first writes integers
  in a hyperslab selection to a dataset with dataspace
  dimensions of DIM_XxDIM_Y, then closes the file.  Next, it
  reopens the file, reads back the data, and outputs it to
  the screen.  Finally it reads the data again using a
  different hyperslab selection, and outputs the result to
  the screen.
 ************************************************************/
package org.nersc.io

import ncsa.hdf.hdf5lib.H5
import ncsa.hdf.hdf5lib.HDF5Constants
import org.slf4j.LoggerFactory

import ncsa.hdf.`object`.Dataset
import ncsa.hdf.`object`.HObject
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.SparkContext._

object read {

  /**
   * Gets an NDimensional array of from a hdf
   * @param FILENAME where the hdf file is located
   * @param DATASETNAME the hdf variable to search for
   * @return
   */

  //def readone(FILENAME:String, DATASETNAME:String): (Array[Array[Float]])= {
    def readone(x:String): (Array[Array[Float]])= {
    val para =x.split(",")
    val FILENAME=para{0}
    val DATASETNAME=para{1}
    var DIM_X: Int = 4
    var DIM_Y: Int = 3
    var RANK: Int = 2
    val logger = LoggerFactory.getLogger(getClass)
    var file_id = -1
    var dataset_id = -1
    var dset_data = Array.ofDim[Float](DIM_X, DIM_Y)

    //Open an existing file.
    file_id = H5.H5Fopen(FILENAME, HDF5Constants.H5F_ACC_RDONLY, HDF5Constants.H5P_DEFAULT)
    if (file_id < 0) logger.info("file open error" + FILENAME)

    //Open an existing dataset.
    dataset_id = H5.H5Dopen(file_id, DATASETNAME, HDF5Constants.H5P_DEFAULT)
    if (dataset_id < 0) logger.info("file open error" + FILENAME)
    //var dset =(Dataset) H5File(file_id)

    // Read the data using the default properties.
    var dread_id = -1
    if (dataset_id >= 0){
       dread_id = H5.H5Dread(dataset_id, HDF5Constants.H5T_NATIVE_FLOAT,
          HDF5Constants.H5S_ALL, HDF5Constants.H5S_ALL,
          HDF5Constants.H5P_DEFAULT, dset_data)
    }
    if(dread_id<0)
     logger.info("dataset open error" + FILENAME)
    //println(dset_data.deep.mkString("\n")) 
   
    dset_data
  }


  def main(args: Array[String]): Unit = {
    var Filename="1.h5"
    var Datasetname="test" 
    var dset = Array.ofDim[Float](4, 3)
    //dset=read.readone(Filename, Datasetname)
    //println(dset.deep.mkString("\n"))
   
    val masterURL = if (args.length <= 1) "local[2]" else args(1)
    val partitions = if (args.length <= 2) 2 else args(2).toInt
    val dimension = if (args.length <= 3) (4, 3) else (args(3).toInt, args(3).toInt)
    val variable = if (args.length <= 4) "test" else args(4)
    val hdfspath = if (args.length <= 5) "resources/hdf5/1.h5" else args(5)
    val csvfile = if(args.length <= 6) "resources/hdf5/scalafilelist" else args(6)


    val sparkConf = new SparkConf().setAppName("h5spark").setMaster(masterURL)
    val sc =new SparkContext(sparkConf)
    val file_path =  sc.textFile(csvfile,minPartitions=partitions)

    val dsetrdd =file_path.flatMap(read.readone)
    dsetrdd.cache()
    println(dset.deep.mkString("\n"))
  }

}
