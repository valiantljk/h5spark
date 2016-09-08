/************************************************************
HDF5 writer in spark
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

import org.scalatest.FunSpec
import org.specs2.mock._
import org.specs2.mutable.Specification
import org.mockito.Mockito.doNothing
import org.mockito.Moclito._
import scala.io.BufferedSource

object writeSpec extends Specification with Mockito  {
   val file =mock[File]
   file.getName returns "file"

   "write#h5write_array" should {
     val content =RDD[Array[Double]] 

    "return false if file exists and should not be overwritten" in {
      val writeFile = write()
      file.exists() return true
      val actual = writeFile.h5write_array(file,content, overwrite = false) 
      actual must beFalse
     }
   }

}

