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
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Counts words in new text files created in the given directory
 * Usage: HdfsByteCount <directory>
 *   <directory> is the directory that Spark Streaming will use to find and read new text files.
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
    
    // Create the context. Check every 2 second.
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create the FileInputDStream on the directory and use the
    // stream to count bytes in new files created
    val bytes = ssc.binaryRecordsStream(args(0), 1)
    var count = bytes.count()
    count.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
// scalastyle:on println
