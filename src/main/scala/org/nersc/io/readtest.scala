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
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._

object readtest {
 def main(args: Array[String]): Unit = {

   if(args.length <6) {
	println("arguments less than 6")
	System.exit(1);
    }
    var logger = LoggerFactory.getLogger(getClass)
    var csvfile =  args(0)
    var partitions = args(1).toInt
    var repartition = args(2).toInt
    var input = args(3)
    var variable = args(4)
    var rows = args(5).toInt
    var sparkConf = new SparkConf().setAppName("h5spark-scala")
    val sc =new SparkContext(sparkConf)
    val dsetrdd =  sc.textFile(csvfile,minPartitions=partitions)
    val pardd=dsetrdd.repartition(repartition) 

    //java version
    //import org.nersc.io._
    //val rdd=pardd.flatMap(hyperRead.readHyperslab)

    //scala version
    val rdd=pardd.flatMap(read.readonep)
    rdd.cache()
    var xcount= rdd.count()
    logger.info("\nRDD Count: "+xcount+" , Total number of rows of all hdf5 files\n")
    //logger.info("\nRDD First: "+rdd.first())
    //rdd.collect()(0).foreach(println)
    sc.stop()
  }

}
