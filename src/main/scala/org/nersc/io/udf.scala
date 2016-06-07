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

object udf {
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

  def maper3D(V:Double,K:Int): List[(Int, Double)]= { 
    var move_direction = Array(0, 1, 2, 3, 4, 5, 6)
    var coordinate = rowmajor_l_reverse (K)
    var box_id:Int= 1
    var boxv: List[(Int, Double)] = List()
    for (i<- Range(0, move_direction.length ) ){
      var new_coordinate = move_coordinate(coordinate, move_direction(i))
      // Use the lineried offset as id
      box_id = rowmajor_l(new_coordinate)
      boxv = (box_id,V)::boxv
    }
    //(box_id,V)
    boxv
  }


}

