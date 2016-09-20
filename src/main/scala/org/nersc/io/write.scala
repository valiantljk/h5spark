/************************************************************
HDF5 reader in spark
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
import scala.collection.immutable.NumericRange


//object write {
  /* the h5write_array take one RDD partition
   * the structure of RDD is essential for this function, e.g., it can be RDD[Array[Double]], or RDD[value: Double, index: Long]
   * and figure out the hyperslab for writing that partition into the hdf5 file 
   * h5write_2d is to convert the RDD into a 2D dataset in HDF5. 
   */


  //def h5write_1d()
//  def h5write(sc:SparkContext,inpath:File,variable:String, partitions: Long, content: RDD[Array[Double],Long], overwrite: Boolean])
//  def h5write_2d(sc: SparkContext, inpath: File, variable: String, partitions: Long,content: RDD[Double],overwrite: Boolean):Boolean = {
//    true
//  }
  //def h5write_3d(){} 
//}
