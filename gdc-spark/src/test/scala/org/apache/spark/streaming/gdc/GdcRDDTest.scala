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

package org.apache.spark.streaming.gdc

import java.util.Date

import es.alvsanand.gdc.core.downloader.{GdcDownloaderParameters, GdcFile}
import es.alvsanand.gdc.core.util.{GdcDownloaderFactoryHelper, SparkTest}

class GdcRDDTest extends SparkTest {
  it should "return one partition" in {
    val file = GdcFile("/files/example.txt", new Date)

    val rdd = new GdcRDD(sc, Array(file), GdcDownloaderFactoryHelper.createDownloaderFactory(Seq
    (file)), GdcDownloaderParameters())

    rdd.partitions should be(Array(new GdcRDDPartition(file, 0)))
  }

  it should "return three partition" in {
    val files = Array(
      GdcFile("/files/example.txt", new Date),
      GdcFile("/files/example_20161201.txt", new Date),
      GdcFile("/files/example_20161202.txt", new Date))

    val rdd = new GdcRDD(sc, files, GdcDownloaderFactoryHelper.createDownloaderFactory(files),
      GdcDownloaderParameters())
    val partitions = rdd.partitions

    partitions.size should be(3)
    partitions(0).asInstanceOf[GdcRDDPartition[GdcFile]].gdcFile should be(files(0))
    partitions(0).asInstanceOf[GdcRDDPartition[GdcFile]].index should be(0)
    partitions(1).asInstanceOf[GdcRDDPartition[GdcFile]].gdcFile should be(files(1))
    partitions(1).asInstanceOf[GdcRDDPartition[GdcFile]].index should be(1)
    partitions(2).asInstanceOf[GdcRDDPartition[GdcFile]].gdcFile should be(files(2))
    partitions(2).asInstanceOf[GdcRDDPartition[GdcFile]].index should be(2)
  }

  it should "test simple File" in {
    val file = GdcFile("/files/example.txt", new Date)

    val rdd = new GdcRDD(sc, Array(file), GdcDownloaderFactoryHelper.createDownloaderFactory(Seq
    (file)), GdcDownloaderParameters())

    rdd.count() should be(5)
    rdd.collect() should be(Array("LINE 001", "LINE 002", "LINE 003", "LINE 004", "LINE 005"))
  }

  it should "test multiple Files" in {
    val files = Array(
      GdcFile("/files/example_20161201.txt", new Date),
      GdcFile("/files/example_20161202.txt", new Date))

    val rdd = new GdcRDD(sc, files, GdcDownloaderFactoryHelper.createDownloaderFactory(files),
      GdcDownloaderParameters())

    rdd.count() should be(10)
    rdd.collect() should be(Array("LINE 001 - 20161201", "LINE 002 - 20161201", "LINE 003 - " +
      "20161201", "LINE 004 - " +
      "20161201", "LINE 005 - 20161201",
      "LINE 001 - 20161202", "LINE 002 - 20161202", "LINE 003 - 20161202", "LINE 004 - 20161202",
      "LINE 005 - " +
        "20161202"))
  }

  it should "test simple File with enough retries" in {
    val file = GdcFile("/files/example.txt", new Date)

    val rdd = new GdcRDD(sc, Array(file), GdcDownloaderFactoryHelper.createDownloaderFactory(Seq
    (file),
      downloadBadTries = 2), GdcDownloaderParameters(), maxRetries = 2)

    rdd.count() should be(5)
    rdd.collect() should be(Array("LINE 001", "LINE 002", "LINE 003", "LINE 004", "LINE 005"))
  }

  it should "test simple File with not enough retries" in {
    val file = GdcFile("/files/example.txt", new Date)

    intercept[org.apache.spark.SparkException] {
      new GdcRDD(sc, Array(file), GdcDownloaderFactoryHelper.createDownloaderFactory(Seq(file),
        downloadBadTries =
        2), GdcDownloaderParameters(), maxRetries = 1).collect()
    }
  }

  it should "test bad File" in {
    val file = GdcFile("BAD_FILE", new Date)

    intercept[org.apache.spark.SparkException] {
      new GdcRDD(sc, Array(file), GdcDownloaderFactoryHelper.createDownloaderFactory(Seq(file)),
        GdcDownloaderParameters()).collect()
    }
  }

  it should "test simple GZ File" in {
    val file = GdcFile("/files/example.txt.gz", new Date)

    val rdd = new GdcRDD(sc, Array(file), GdcDownloaderFactoryHelper.createDownloaderFactory(Seq
    (file)), GdcDownloaderParameters())

    rdd.count() should be(5)
    rdd.collect() should be(Array("LINE 001", "LINE 002", "LINE 003", "LINE 004", "LINE 005"))
  }
}
