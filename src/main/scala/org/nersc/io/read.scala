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

import ncsa.hdf.hdf5lib.H5._
import ncsa.hdf.hdf5lib.HDF5Constants._
import ncsa.hdf.hdf5lib.exceptions.HDF5LibraryException
import ncsa.hdf.hdf5lib.exceptions.HDF5Exception
import org.slf4j.LoggerFactory
import java.io.File

import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix
import org.apache.spark.mllib.linalg.distributed.IndexedRow
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.SparkContext

object read {
  private def getdimentions(file: String, variable: String): Array[Long] = {
    val logger = LoggerFactory.getLogger(getClass)
    var file_id = -2
    var dataset_id = -2
    var dataspace_id = -2
    try {
      file_id = H5Fopen(file, H5F_ACC_RDONLY, H5P_DEFAULT)
      dataset_id = H5Dopen(file_id, variable, H5P_DEFAULT)
    }
    catch {
      case e: Exception => logger.info("\nFile error: " + file)
    }
    dataspace_id = H5Dget_space(dataset_id)
    val ranks = H5Sget_simple_extent_ndims(dataspace_id)
    val dset_dims = new Array[Long](ranks)
    H5Sget_simple_extent_dims(dataspace_id, dset_dims, null)

    H5Sclose(dataspace_id)
    H5Dclose(dataset_id)
    H5Fclose(file_id)
    dset_dims
  }

  private def getListOfFiles(dir: File, extensions: List[String]): List[File] = {
    dir.listFiles.filter(_.isFile).toList.filter { file =>
      extensions.exists(file.getName.endsWith(_))
    }
  }

  private def read_whole_dataset(FILENAME: String, DATASETNAME: String): (Array[Array[Double]]) = {
    val logger = LoggerFactory.getLogger(getClass)
    var file_id = -2
    var dataset_id = -2
    var dataspace_id = -2
    var dset_dims = new Array[Long](2)
    dset_dims = Array(1, 1)

    //Open file
    try {
      file_id = H5Fopen(FILENAME, H5F_ACC_RDONLY, H5P_DEFAULT)
    }
    catch {
      case e: Exception => logger.info("\nFile open error,filename:" + FILENAME)
    }
    //Open dataset/variable
    try {
      dataset_id = H5Dopen(file_id, DATASETNAME, H5P_DEFAULT)
    }
    catch {
      case e: Exception => logger.info("\nDataset open error:" + DATASETNAME)
    }
    //Get dimension info
    try {
      dataspace_id = H5Dget_space(dataset_id)
      H5Sget_simple_extent_dims(dataspace_id, dset_dims, null)
    }
    catch {
      case e: Exception => logger.info("Dataspace open error,dataspace_id: " + dataspace_id)
    }
    if (dset_dims(0) > 0 && dset_dims(1) > 0) {
      val dset_data = Array.ofDim[Double](dset_dims(0).toInt, dset_dims(1).toInt)
      var dread_id = -1
      try {
        dread_id = H5Dread(dataset_id, H5T_NATIVE_DOUBLE,
          H5S_ALL, H5S_ALL,
          H5P_DEFAULT, dset_data)
      }
      catch {
        case e: java.lang.NullPointerException => logger.info("data object is null")
        case e@(_: HDF5LibraryException | _: HDF5Exception) =>
          logger.info("Error from HDF5 library|Failure in the data conversion. Read error info: " +
            e.getMessage + e.printStackTrace)
        case e: java.lang.NegativeArraySizeException => logger.info("emptyjavaarray" + e.getMessage + e.printStackTrace + FILENAME)
      }
      if (dread_id < 0)
        logger.info("Dataset open error" + FILENAME)
      dset_data
    }
    else {
      val dset_data = Array.ofDim[Double](1, 1)
      logger.info("file empty" + FILENAME)
      dset_data
    }
  }

  private def read_hyperslab(FILENAME: String, DATASETNAME: String, start: Long, end: Long): (Array[Double], Array[Int]) = {
    val logger = LoggerFactory.getLogger(getClass)
    var file_id = -2
    var dataset_id = -2
    var dataspace_id = -2
    var ranks: Int = 2
    var end1 = end
    /*Open an existing file*/
    try {
      file_id = H5Fopen(FILENAME, H5F_ACC_RDONLY, H5P_DEFAULT)
    }
    catch {
      case e: Exception => logger.info("\nFile error: " + FILENAME)
    }
    /*Open an existing dataset/variable*/
    try {
      dataset_id = H5Dopen(file_id, DATASETNAME, H5P_DEFAULT)
    }
    catch {
      case e: Exception => logger.info("\nDataset error\n")
    }
    //Get dimension information of the dataset
    var dset_dims = new Array[Long](2)
    try {
      dataspace_id = H5Dget_space(dataset_id)
      ranks = H5Sget_simple_extent_ndims(dataspace_id)
      dset_dims = new Array[Long](ranks)
      H5Sget_simple_extent_dims(dataspace_id, dset_dims, null)
    }
    catch {
      case e: Exception => logger.info("\nDataspace error")
    }
    //Adjust last access
    if (end1 > dset_dims(0))
      end1 = dset_dims(0)
    val step = end1 - start
    var subset_length: Long = 1
    logger.info("Ranks="+ranks)
    logger.info("Dim 0="+dset_dims(0))
    for (i <- 1 to ranks-1) {
      subset_length *= dset_dims(i)
      logger.info("Dim "+i+"="+dset_dims(i))
    }

    val dset_datas = Array.ofDim[Double](step.toInt * subset_length.toInt)

    val start_dims: Array[Long] = new Array[Long](ranks)
    val count_dims: Array[Long] = new Array[Long](ranks)
    start_dims(0) = start.toLong
    count_dims(0) = step.toLong
    for (i <- 1 to ranks-1) {
      start_dims(i) = 0.toLong
      count_dims(i) = dset_dims(i)
    }
    var hyper_id = -2
    var dread_id = -2
    var memspace = -2
    H5Sclose(dataspace_id)
    /*read data*/
    try {
      dataspace_id = H5Dget_space(dataset_id)
      memspace = H5Screate_simple(ranks, count_dims, null)
      hyper_id = H5Sselect_hyperslab(dataspace_id, H5S_SELECT_SET, start_dims, null, count_dims, null)
      dread_id = H5Dread(dataset_id, H5T_NATIVE_DOUBLE, memspace, dataspace_id, H5P_DEFAULT, dset_datas)
    }
    catch {
      case e: java.lang.NullPointerException => logger.info("data object is null")
      case e@(_: HDF5LibraryException | _: HDF5Exception) =>
        logger.info("Error from HDF5 library|Failure in the data conversion. Read error info: " + e.getMessage + e.printStackTrace)
    }
    var global_start = (start - 1) * subset_length
    if (global_start < 0) global_start = 0
    var global_end = end1 * subset_length
    import Array._
    val index: Array[Int] = range(global_start.toInt, global_end.toInt, 1)
    (dset_datas, index)
  }

  private def read_array(FILENAME: String, DATASETNAME: String, start: Long, end: Long): (Array[Array[Double]]) = {
    val dset_dims: Array[Long] = getdimentions(FILENAME, DATASETNAME)
    var (dset_datas: Array[Double], index: Array[Int]) = read_hyperslab(FILENAME, DATASETNAME, start, end)
    var dset_data: Array[Array[Double]] = Array.ofDim((end - start).toInt, (index(-1) - index(0)))
    var end1 = end
    if (end1 > dset_dims(1))
      end1 = dset_dims(1)
    for (id <- 0 to (end1 - start).toInt - 1) {
      for (jd <- 0 to dset_dims(1).toInt - 1) {
        dset_data(id)(jd) = dset_datas(id * dset_dims(1).toInt + jd)
      }
    }
    dset_data
  }


  //  global_array_dims=3
  //  global_array_size=[1000. 1000. 1000]
  //  #
  //  #
  //  # Direction viewed from top-down (z)
  //  #      1
  //  #  2   5(0, 6)    4
  //  #      3
  //  # (0)  is myselft
  //  # (5)  is the one above me
  //  # (6)  is the one below conrrent
  //move_direction =[0, 1, 2, 3, 4, 5, 6]

  def rowmajor_l_reverse(index: Int): Array[Int] ={
    //convert index to 3D coordinate
    var global_array_dims = 3
    var indexi=index
    var global_array_size= Array(2000,2000,800)
    var coordinate = new Array[Int](3)
    for (i <- Range(0,global_array_dims).reverse ){
      coordinate:+ (indexi % global_array_size(i))
      indexi = indexi / global_array_size(i)
    }
    coordinate
  }

  def rowmajor_l(coordinate:Array[Int]): Int= {
    //linear coordinate
    var box_id = coordinate(0)
    var global_array_size= Array(2000,2000,800)
    for (i <- Range(1, coordinate.length - 1)) {
      box_id = box_id * global_array_size(i) + coordinate(i)
    }
    box_id
  }
  def move_coordinate(coordinate: Array[Int], direction: Int):Array[Int] = {
    var new_coordinate = coordinate
    var global_array_size= Array(2000,2000,800)
    if (direction != 0){
      if (direction != 1){
        if (new_coordinate(0) - 1 >= 0){
          new_coordinate(0) = new_coordinate(0) - 1
        }else{
          new_coordinate(0) = 0
        }
      }else if (direction != 2){
        if (new_coordinate(1) - 1 >= 0){
          new_coordinate(1) = new_coordinate(1) - 1
        }else{
          new_coordinate(1) = 0
        }
      }else if (direction != 3){
        if (new_coordinate(0) + 1 > global_array_size(0)){
          new_coordinate(0) = global_array_size(0)
        }else{
          new_coordinate(0) = new_coordinate(0) + 1
        }
      }else if (direction != 4){
        if(new_coordinate(1) + 1 > global_array_size(1)){
          new_coordinate(1) = global_array_size(1)
        }else{
          new_coordinate(1) = new_coordinate(1) + 1
        }
      }else if (direction != 5){
        if (new_coordinate(2) + 1 > global_array_size(2)){
          new_coordinate(2) = global_array_size(2)
        }else{
          new_coordinate(2) = new_coordinate(2) + 1
        }
      }else if (direction != 6){
        if (new_coordinate(2) - 1 >= 0){
          new_coordinate(2) = new_coordinate(2) - 1
        }else{
          new_coordinate(2) = 0
        }
      }else{
        print("Un-defined direction")
      }
    }
    new_coordinate
  }

  def maper3D(V:Double,K:Int): (Int, Double)= { 
    var move_direction = Array(0, 1, 2, 3, 4, 5, 6)
    var coordinate = rowmajor_l_reverse (K)
    var box_id:Int= 1

    for (i<- Range(0, move_direction.length ) ){
      var new_coordinate = move_coordinate(coordinate, move_direction(i))
      // Use the lineried offset as id
      box_id = rowmajor_l(new_coordinate)
    }
    (box_id,V)
  }

  def h5read_point(sc:SparkContext, inpath: String, variable: String, partitions: Long): RDD[(Double,Int)] = {
    val file = new File(inpath)
    val logger = LoggerFactory.getLogger(getClass)
    if (file.exists && file.isFile) {
      //read single file
      logger.info("Read Single file:" + inpath)
      val dims: Array[Long] = getdimentions(inpath, variable)
      val rows: Long = dims(0)
      var num_partitions: Long = partitions
      if (rows < num_partitions) {
        num_partitions = rows
      }
      val step: Long = rows / num_partitions
      val arr = sc.range(0, rows, step, partitions.toInt).flatMap(x =>
        read_hyperslab(inpath, variable, x, x + step)._1 zip read_hyperslab(inpath, variable, x, x + step)._2)
      arr
    }
    else {
      val okext = List("h5", "hdf5")
      val listf = getListOfFiles(file, okext)
      logger.info("Read" + listf.length + " files from directory:" + inpath)
      val arr = sc.parallelize(listf, partitions.toInt).map(x => x.toString).flatMap(x =>
        read_hyperslab(x, variable,0,1e100.toLong)._1 zip read_hyperslab(x, variable,0,1e100.toLong)._2)
      arr
    }
  }

  def h5read_array(sc: SparkContext, inpath: String, variable: String, partitions: Long): RDD[Array[Double]] = {
    val file = new File(inpath)
    val logger = LoggerFactory.getLogger(getClass)
    if (file.exists && file.isFile) {
      //read single file
      logger.info("Read Single file:" + inpath)
      val dims: Array[Long] = getdimentions(inpath, variable)
      val rows: Long = dims(0)
      var num_partitions: Long = partitions
      if (rows < num_partitions) {
        num_partitions = rows
      }
      val step: Long = rows / num_partitions
      val arr = sc.range(0, rows, step, partitions.toInt).flatMap(x => read_array(inpath, variable, x, x + step))
      arr
    }
    else {
      val okext = List("h5", "hdf5")
      val listf = getListOfFiles(file, okext)
      logger.info("Read" + listf.length + " files from directory:" + inpath)
      val arr = sc.parallelize(listf, partitions.toInt).map(x => x.toString).flatMap(x => read_whole_dataset(x, variable))
      arr
    }
  }

  def h5read_vec(sc: SparkContext, inpath: String, variable: String, partitions: Long): RDD[DenseVector] = {
    val arr = h5read_array(sc, inpath, variable, partitions).map {
      case a: Array[Double] =>
        new DenseVector(a)
    }
    arr
  }

  def h5read_irow(sc: SparkContext, inpath: String, variable: String, partitions: Long): RDD[IndexedRow] = {
    val irow = h5read_vec(sc, inpath, variable, partitions).zipWithIndex().map(k => (k._1, k._2)).map(k => new IndexedRow(k._2, k._1))
    irow
  }

  def h5read_imat(sc: SparkContext, inpath: String, variable: String, partitions: Long): IndexedRowMatrix = {
    val irow = h5read_irow(sc, inpath, variable, partitions)
    val imat = new IndexedRowMatrix(irow)
    imat
  }

}

