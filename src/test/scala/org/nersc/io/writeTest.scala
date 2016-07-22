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
//import org.specs2.mock.Mockito._
import scala.io.BufferedSource
import org.specs2.mock._
import org.specs2.mutable.Specification

object writeSpec extends Specification with Mockito{
   val file = mock[File]
   file.getName returns "file"
   val sc =new SparkContext()
   val content = Array(1.1,2.2,3.3)
   val variable = "temperature"
   val content_RDD = sc.parallelize(content)   

   "write#h5write_array" should {
 

     "return false if file exists and should not be overwritten" in {
       val writeFile = write
       file.exists() returns true
       //sc: org.apache.spark.SparkContext, inpath: String, variable: String, partitions: Long, content:RDD[Double],overwrite:Boolean
       val actual = writeFile.h5write_array(sc,file,variable,1,content_RDD,overwrite = false) 
       actual must beFalse
     }
   }

}

