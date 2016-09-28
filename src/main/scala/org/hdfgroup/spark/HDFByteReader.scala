/*
   Copyright (C) 2016 The HDF Group
   All rights reserved
*/

package org.hdfgroup.spark
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkContext
import org.elasticsearch.spark.rdd.EsSpark                        
import org.elasticsearch.spark._
import org.apache.spark.sql._
import org.elasticsearch.spark.sql._
import java.nio.ByteBuffer
import java.lang.Float
import java.io._
import java.util.zip.InflaterInputStream

/**
 * Read bytes (in chunk) from an HDF file.
 * Usage: HDFByteReader <filename> <offset> <length> <filter>
 *   <filename> is the name of HDF file.
 *   <offset> is the starting position in file.
 *   <length> is the number of bytes to read.
 *   <filter> indicates whether inflate decompression is necessary.
 *
 * To run this on your local machine on directory `localdir`, run this example
 *    $ bin/run-example \
 *       org.hdfgroup.spark.HDFByteReader filename offset length filter
 *
 */
object HDFByteReader {


  def b2s(a: Array[Byte]): String = {
      val f = ByteBuffer.wrap(a).getFloat
      Float.toString(f)
  }
       
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: HDFByteReader <file> <offset> <length> <decompression>")
      System.exit(1)
    }
    var arr = Array.fill[Byte](args(2).toInt)(0)
    var in = None: Option[RandomAccessFile]

    var out = None: Option[FileOutputStream]
    val sparkConf = new SparkConf().setAppName("HDFByteReader")
    sparkConf.set("es.nodes", "giraffe") // change giraffe to your ES server.
    try {
        println("Opening file: "+args(0))
        in = Some(new RandomAccessFile(args(0), "r"))
        in.get.seek(args(1).toLong)
        out = Some(new FileOutputStream("/tmp/test.bin"))

        in.get.read(arr)
        println(arr.mkString(" "))        
        if (args(3) == "1" ) {
           println("Decompressing GZIP stream.")
           var gzis = new InflaterInputStream(new ByteArrayInputStream(arr))
           var reader = new InputStreamReader(gzis)
           var in2 = new BufferedReader(reader)
           var c = 0
           while ({c = in2.read(); c != -1}) {
              out.get.write(c)
              println(c) // It should print 4.
           }
        }
        else {
           out.get.write(arr)
        }

        
    } catch {
      case e: IOException => e.printStackTrace
    } finally {
        println("entered finally...")
        if (in.isDefined) in.get.close
        if (out.isDefined) out.get.close
    }
    
  }
}

// References
//
// [1] https://www.safaribooksonline.com/library/view/scala-cookbook/9781449340292/ch12s04.html