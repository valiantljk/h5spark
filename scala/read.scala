package org.apache.spark.h5spark
/**
  * Created by jialin on 3/2/16.
  */

// basic Spark
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Logging


//HDF5 Support
import java.util.EnumSet
import java.util.HashMap
import java.util.Map
import java.io.File
import java.io.Serializable
import ncsa.hdf.`object`.FileFormat
import ncsa.hdf.`object`.Dataset
import ncsa.hdf.`object`.Datatype
import ncsa.hdf.`object`.Group
import ncsa.hdf.`object`.h5.H5File
import ncsa.hdf.`object`
import scala.util.Try
import scala.reflect.ClassTag
import collection.JavaConversions._
import collection.mutable._


object read{

  def readone[T](filename:String, varname:String):List[T]={

        FileFormat fileFormat=FileFormat.getFileFormat(FileFormat.FILE_TYPE_HDF5);
        if(fileFormat == null) {
          System.err.println("cannot find HDF5 FileFormat");
          return;
        }

        FileFormat testFile= fileFormat.createInstance(filename,FileFormat.WRITE);

        if(testFile==null){
          System.err.println("Failed to open File: "+ filename);
          return;
        }
        //testFile
        //Group root =(Group) testFile
        //Group root =(Group) testFile
  }
}
