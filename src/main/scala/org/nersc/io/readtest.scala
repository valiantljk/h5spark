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
    val csvfile =  args(0)
    val partitions = args(1).toInt
    val repartition = args(2).toInt
    val input = args(3)
    val variable = args(4)
    val rows = args(5).toInt
    val sparkConf = new SparkConf().setAppName("h5spark-scala")
    val sc =new SparkContext(sparkConf)
    val dsetrdd =  sc.textFile(csvfile,minPartitions=partitions)
    val pardd=dsetrdd.repartition(repartition)
    //val s=  new hyperRead
    import org.nersc.io._
    //val test=hyperRead.readHyperslab()  
    //println(test.deep.mkString("\n"))
   //org.nersc.io2.hyperRead.readHyperslab("/global/cscratch1/sd/jialin/climate/oceanTemps.hdf5,temperatures,4583256,4585372")
    val rdd=pardd.flatMap(hyperRead.readHyperslab)
    //val dsetrdd = file_path.flatMap(readHyperslab())
    rdd.cache()
    //dsetrdd.count()
    var xcount= rdd.count()
    println("\nRDD Count: "+xcount+" , Total number of rows of all hdf5 files\n")
    //println(dset.deep.mkString("\n"))
    sc.stop()
  }

}
