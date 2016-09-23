/*
   Copyright (C) 2016 The HDF Group
   All rights reserved

   This example is based on HdfsWordCount.scala provided by Apache.

*/

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package org.hdfgroup.spark
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkContext
import org.elasticsearch.spark.rdd.EsSpark                        


/**
 * Counts bytes in binary files created in the given directory
 * Usage: HdfsByteCount <directory>
 *   <directory> is the directory that Spark Streaming will use to find and read new binary files.
 *
 * To run this on your local machine on directory `localdir`, run this example
 *    $ bin/run-example \
 *       org.hdfgroup.spark.HdfsByteCount localdir
 *
 * Then create a binary file in `localdir` and the number of the bytes will get counted.
 */
object HdfsByteCount {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: HdfsByteCount <directory>")
      System.exit(1)
    }


    val sparkConf = new SparkConf().setAppName("HdfsByteCount")
    sparkConf.set("es.nodes", "giraffe")

    // Creating multiple contexts will not work with Spark.
    //
    // val sc = new SparkContext(...)
    // val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
    // val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")
    // sc.makeRDD(Seq(numbers, airports)).saveToEs("spark/docs")

    // Create the context. Check every 2 second.
    val ssc = new StreamingContext(sparkConf, Seconds(2))


    // Create the FileInputDStream on the directory and use the
    // stream to count bytes in new files created
    val bytes = ssc.binaryRecordsStream(args(0), 1)
    // val arr = new ArrayBuffer[String]
    bytes.foreachRDD(rdd => {
        if(!rdd.partitions.isEmpty) {
            // rdd.map(r=>Map("data"->r)).saveToES("spark/docs")
            // It doesn't work.                        
            // rdd.saveAsObjectFiles("hdfs://jaguar:9000/test.bin")
            
            // The following doesn't work either.
            // bytes.saveAsObjectFiles("hdfs://jaguar:9000/test.bin")

            // This is the goal but it doesn't work.
            // EsSpark.saveToEs(rdd, "spark/docs")        
            // print(arr)
        }
    })
    val count = bytes.count()
    count.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
// scalastyle:on println
