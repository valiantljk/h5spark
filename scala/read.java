/**
  * Created by jialin on 3/2/16.
  */

// basic Spark
//import org.apache.spark.SparkContext;
//import org.apache.spark.SparkConf;
//import org.apache.spark.Logging;


//java Support
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.io.File;
import java.io.Serializable;

//HDF5
import ncsa.hdf.object.FileFormat;
import ncsa.hdf.object.Dataset;
import ncsa.hdf.object.Datatype;
import ncsa.hdf.object.Group;
//import ncsa.hdf.object.h4;
//import ncsa.hdf.object.h5;
//import ncsa.hdf.object.h5.H5File;
//import ncsa.hdf.object;

public class read{

  private static String filename="1.h5";

  public static void readone(){

        FileFormat fileFormat=FileFormat.getFileFormat(FileFormat.FILE_TYPE_HDF5);
        if(fileFormat == null) {
          System.err.println("cannot find HDF5 FileFormat");
          return;
        }
	else{
	  System.out.println("found the fileformat");
	}

        FileFormat testFile= fileFormat.createFile("1.h5",FileFormat.WRITE);
	//FileFormat testFile=null;
        if(testFile==null){
          System.err.println("Failed to open File: "+ filename);
          return;
        }
        //testFile
        //Group root =(Group) testFile
        //Group root =(Group) testFile
  }

 public static void main(String[] args){
  	read.readone();
 }

}
