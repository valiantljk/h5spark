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
import org.apache.spark.mllib.linalg.DenseVector
object readtest {
 def main(args: Array[String]): Unit = {

   if(args.length <3) {
	println("Arguments less than 3")
	System.exit(1);
    }
    var logger = LoggerFactory.getLogger(getClass)    
    var partition = args(0).toInt
    var input = args(1)
    var variable = args(2)
    //var app_name = args(3) 
    //val sparkConf = new SparkConf().setAppName(app_name)
    //val sparkConf = new SparkConf()
    //val sc =new SparkContext(sparkConf)
    val sc = new SparkContext()
    val rdd0 = read.h5read_point(sc,input,variable,partition)
    //rdd.take(1).foreach(println)
    //rdd=rdd.map(x => println(x._1))
    val rdd=rdd0.map(x => read.maper3D(x._1,x._2))
    //val rdd = read.h5read (sc,input,variable,partition)
    rdd.cache()
    //val count= rdd.count()
     rdd.take(100).foreach(println)
    //logger.info("\nRDD_Count: "+count+" , Total number of rows of all hdf5 files\n")
    //logger.info("\nRDD_First: ")
    //rdd.take(1)(0).toArray.foreach(println)
    sc.stop()
  }

}


/*

    /* h5spark prototyping:

    val rdd        = h5read      (sc,inpath, variable, repartition)
    val indexrow   = h5read_irow (sc,inpath, variable, repartition)
    val indexrowmat= h5read_imat (sc, inpath,variable, repartition)

    */

    //val dsetrdd =  sc.textFile(csvfile,minPartitions=partitions)
    //val pardd=dsetrdd.repartition(repartition)

    //java version
    //import org.nersc.io._
    //val rdd=pardd.flatMap(hyperRead.readHyperslab)

    //scala version
    /*val rdd=pardd.flatMap(read.readonep).map{
        case x:Array[Double]=>
        new DenseVector(x)
    }
    */

*/
