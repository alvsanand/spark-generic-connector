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

package org.apache.spark.rdd.gdc

import org.apache.spark.util.gdc.{DownloadFile, DownloaderFactoryHelper, SparkTest}

class DownloadContextTest extends SparkTest {

  it should "Return all" in {
    val files = Array(
      DownloadFile("/files/example.txt"),
      DownloadFile("/files/example_20161201.txt", dt.parse("2016-10-12 00:00:00")),
      DownloadFile("/files/example_20161202.txt", dt.parse("2016-10-13 00:00:00")))

    val rdd = sc.createDownloadRDD(DownloaderFactoryHelper.createDownloaderFactory(files), Map())
    val partitions = rdd.partitions

    partitions.size should be(3)
    partitions(0).asInstanceOf[DownloadRDDPartition[DownloadFile]].downloadFile should be(files(0))
    partitions(0).asInstanceOf[DownloadRDDPartition[DownloadFile]].index should be(0)
    partitions(1).asInstanceOf[DownloadRDDPartition[DownloadFile]].downloadFile should be(files(1))
    partitions(1).asInstanceOf[DownloadRDDPartition[DownloadFile]].index should be(1)
    partitions(2).asInstanceOf[DownloadRDDPartition[DownloadFile]].downloadFile should be(files(2))
    partitions(2).asInstanceOf[DownloadRDDPartition[DownloadFile]].index should be(2)
  }

  it should "Return all files with date filter A" in {
    val files = Array(
      DownloadFile("/files/example.txt"),
      DownloadFile("/files/example_20161201.txt", dt.parse("2016-10-12 00:00:00")),
      DownloadFile("/files/example_20161202.txt", dt.parse("2016-10-13 00:00:00")))

    val rdd = sc.createDownloadRDD(DownloaderFactoryHelper.createDownloaderFactory(files), Map(), Option(dt.parse("2016-10-12 00:00:00")), Option(dt.parse("2016-10-13 00:00:01")))
    val partitions = rdd.partitions

    partitions.size should be(2)
    partitions(0).asInstanceOf[DownloadRDDPartition[DownloadFile]].downloadFile should be(files(1))
    partitions(0).asInstanceOf[DownloadRDDPartition[DownloadFile]].index should be(0)
    partitions(1).asInstanceOf[DownloadRDDPartition[DownloadFile]].downloadFile should be(files(2))
    partitions(1).asInstanceOf[DownloadRDDPartition[DownloadFile]].index should be(1)
  }

  it should "Return all files with date filter B" in {
    val files = Array(
      DownloadFile("/files/example.txt"),
      DownloadFile("/files/example_20161201.txt", dt.parse("2016-10-12 00:00:00")),
      DownloadFile("/files/example_20161202.txt", dt.parse("2016-10-13 00:00:00")))

    val rdd = sc.createDownloadRDD(DownloaderFactoryHelper.createDownloaderFactory(files), Map(), None, Option(dt.parse("2016-10-13 00:00:01")))
    val partitions = rdd.partitions

    partitions.size should be(2)
    partitions(0).asInstanceOf[DownloadRDDPartition[DownloadFile]].downloadFile should be(files(1))
    partitions(0).asInstanceOf[DownloadRDDPartition[DownloadFile]].index should be(0)
    partitions(1).asInstanceOf[DownloadRDDPartition[DownloadFile]].downloadFile should be(files(2))
    partitions(1).asInstanceOf[DownloadRDDPartition[DownloadFile]].index should be(1)
  }

  it should "Return all files with date filter C" in {
    val files = Array(
      DownloadFile("/files/example.txt"),
      DownloadFile("/files/example_20161201.txt", dt.parse("2016-10-12 00:00:00")),
      DownloadFile("/files/example_20161202.txt", dt.parse("2016-10-13 00:00:00")))

    val rdd = sc.createDownloadRDD(DownloaderFactoryHelper.createDownloaderFactory(files), Map(), Option(dt.parse("2016-10-12 00:00:00")), None)
    val partitions = rdd.partitions

    partitions.size should be(2)
    partitions(0).asInstanceOf[DownloadRDDPartition[DownloadFile]].downloadFile should be(files(1))
    partitions(0).asInstanceOf[DownloadRDDPartition[DownloadFile]].index should be(0)
    partitions(1).asInstanceOf[DownloadRDDPartition[DownloadFile]].downloadFile should be(files(2))
    partitions(1).asInstanceOf[DownloadRDDPartition[DownloadFile]].index should be(1)
  }

  it should "Return first file with date filter" in {
    val files = Array(
      DownloadFile("/files/example.txt"),
      DownloadFile("/files/example_20161201.txt", dt.parse("2016-10-12 00:00:00")),
      DownloadFile("/files/example_20161202.txt", dt.parse("2016-10-13 00:00:00")))

    val rdd = sc.createDownloadRDD(DownloaderFactoryHelper.createDownloaderFactory(files), Map(), Option(dt.parse("2016-10-12 00:00:00")), Option(dt.parse("2016-10-12 23:59:59")))
    val partitions = rdd.partitions

    partitions.size should be(1)
    partitions(0).asInstanceOf[DownloadRDDPartition[DownloadFile]].downloadFile should be(files(1))
    partitions(0).asInstanceOf[DownloadRDDPartition[DownloadFile]].index should be(0)
  }

  it should "Return last file with date filter" in {
    val files = Array(
      DownloadFile("/files/example.txt"),
      DownloadFile("/files/example_20161201.txt", dt.parse("2016-10-12 00:00:00")),
      DownloadFile("/files/example_20161202.txt", dt.parse("2016-10-13 00:00:00")))

    val rdd = sc.createDownloadRDD(DownloaderFactoryHelper.createDownloaderFactory(files), Map(), Option(dt.parse("2016-10-12 23:59:59")), Option(dt.parse("2016-10-13 00:00:01")))
    val partitions = rdd.partitions

    partitions.size should be(1)
    partitions(0).asInstanceOf[DownloadRDDPartition[DownloadFile]].downloadFile should be(files(2))
    partitions(0).asInstanceOf[DownloadRDDPartition[DownloadFile]].index should be(0)
  }

  it should "Return no files with date filter" in {
    val files = Array(
      DownloadFile("/files/example.txt"),
      DownloadFile("/files/example_20161201.txt", dt.parse("2016-10-12 00:00:00")),
      DownloadFile("/files/example_20161202.txt", dt.parse("2016-10-13 00:00:00")))

    val rdd = sc.createDownloadRDD(DownloaderFactoryHelper.createDownloaderFactory(files), Map(), None, Option(dt.parse("2016-10-11 23:59:59")))
    val partitions = rdd.partitions

    partitions.size should be(0)
  }
}
