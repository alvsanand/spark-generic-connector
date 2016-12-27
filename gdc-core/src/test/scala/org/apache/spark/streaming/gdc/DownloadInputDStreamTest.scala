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

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.gdc.DownloadFileRange
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.util.gdc.{DownloadFile, DownloaderFactoryHelper, SparkTest}

import scala.collection.mutable.ListBuffer

class DownloadInputDStreamTest extends SparkTest {

  val checkPointDirectory = new File(sys.props("java.io.tmpdir"), "FileDownloaderInputDStreamTest")
  val batchTime = Milliseconds(100)
  val checkpointTime = Milliseconds(500)

  def createStopThread(ssc: StreamingContext, sleepTime: Int): Thread = {
    new Thread(new Runnable {
      def run() {
        try {
          Thread.sleep(sleepTime)
        }
        catch {
          case e: Exception =>
        }

        ssc.stop(false)
      }
    })
  }

  it should "process files" in {
    FileUtils.deleteQuietly(new File(checkPointDirectory.getPath))

    val files = Seq(DownloadFile("/files/example_20161201.txt", dt.parse("2016-12-01 00:00:00")),
      DownloadFile("/files/example_20161202.txt", dt.parse("2016-12-02 00:00:00")))
    val date = dt.parse("2016-01-01 00:00:00")

    var data: ListBuffer[String] = ListBuffer();

    val ssc = StreamingContext.getOrCreate(checkPointDirectory.getPath, () => {
      val ssc = new StreamingContext(sc, batchTime)

      val ds = ssc.createDownloadStream(DownloaderFactoryHelper.createDownloaderFactory(files), Map(), Option(DownloadFileRange(date)))

      ds.checkpoint(checkpointTime)

      ssc.checkpoint(checkPointDirectory.getPath)

      ds.foreachRDD { rdd =>
        val d = rdd.collect()
        data ++= d
      }

      ssc
    })

    val stopThread = createStopThread(ssc, 2000)

    stopThread.start()

    ssc.start()

    ssc.awaitTermination()

    data.size should be(10)
    data should be(ListBuffer("LINE 001 - 20161201", "LINE 002 - 20161201", "LINE 003 - 20161201", "LINE 004 - 20161201", "LINE 005 - 20161201",
      "LINE 001 - 20161202", "LINE 002 - 20161202", "LINE 003 - 20161202", "LINE 004 - 20161202", "LINE 005 - 20161202"))
  }

  it should "process files with list failures" in {
    FileUtils.deleteQuietly(new File(checkPointDirectory.getPath))

    val files = Seq(DownloadFile("/files/example_20161201.txt", dt.parse("2016-12-01 00:00:00")),
      DownloadFile("/files/example_20161202.txt", dt.parse("2016-12-02 00:00:00")))
    val date = dt.parse("2016-01-01 00:00:00")

    var data: ListBuffer[String] = ListBuffer();

    val ssc = StreamingContext.getOrCreate(checkPointDirectory.getPath, () => {
      val ssc = new StreamingContext(sc, batchTime)

      val ds = ssc.createDownloadStream(DownloaderFactoryHelper.createDownloaderFactory(files, listBadTries = 1), Map(), Option(DownloadFileRange(date)))

      ds.checkpoint(checkpointTime)

      ssc.checkpoint(checkPointDirectory.getPath)

      ds.foreachRDD { rdd =>
        val d = rdd.collect()
        data ++= d
      }

      ssc
    })

    val stopThread = createStopThread(ssc, 2000)

    stopThread.start()

    ssc.start()

    ssc.awaitTermination()

    data.size should be(10)
    data should be(ListBuffer("LINE 001 - 20161201", "LINE 002 - 20161201", "LINE 003 - 20161201", "LINE 004 - 20161201", "LINE 005 - 20161201",
      "LINE 001 - 20161202", "LINE 002 - 20161202", "LINE 003 - 20161202", "LINE 004 - 20161202", "LINE 005 - 20161202"))
  }

  it should "process files with list failures but not enough retries" in {
    FileUtils.deleteQuietly(new File(checkPointDirectory.getPath))

    val files = Seq(DownloadFile("/files/example_20161201.txt", dt.parse("2016-12-01 00:00:00")),
      DownloadFile("/files/example_20161202.txt", dt.parse("2016-12-02 00:00:00")))
    val date = dt.parse("2016-01-01 00:00:00")

    var data: ListBuffer[String] = ListBuffer();

    val ssc = StreamingContext.getOrCreate(checkPointDirectory.getPath, () => {
      val ssc = new StreamingContext(sc, batchTime)

      val ds = ssc.createDownloadStream(DownloaderFactoryHelper.createDownloaderFactory(files, listBadTries = 100), Map(), Option(DownloadFileRange(date)), maxRetries = 1)

      ds.checkpoint(checkpointTime)

      ssc.checkpoint(checkPointDirectory.getPath)

      ds.foreachRDD { rdd =>

        val d = rdd.collect()
        data ++= d
      }

      ssc
    })

    val stopThread = createStopThread(ssc, 2000)

    stopThread.start()

    ssc.start()

    intercept[org.apache.spark.SparkException] {
      ssc.awaitTermination()
    }

    while (stopThread.isAlive) Thread.sleep(100)

    data.size should be(0)
  }

  it should "process files with download failures" in {
    FileUtils.deleteQuietly(new File(checkPointDirectory.getPath))

    val files = Seq(DownloadFile("/files/example_20161201.txt", dt.parse("2016-12-01 00:00:00")),
      DownloadFile("/files/example_20161202.txt", dt.parse("2016-12-02 00:00:00")))
    val date = dt.parse("2016-01-01 00:00:00")

    var data: ListBuffer[String] = ListBuffer();

    val ssc = StreamingContext.getOrCreate(checkPointDirectory.getPath, () => {
      val ssc = new StreamingContext(sc, batchTime)

      val ds = ssc.createDownloadStream(DownloaderFactoryHelper.createDownloaderFactory(files, downloadBadTries = 1), Map(), Option(DownloadFileRange(date)), maxRetries = 2)

      ds.checkpoint(checkpointTime)

      ssc.checkpoint(checkPointDirectory.getPath)

      ds.foreachRDD { rdd =>
        val d = rdd.collect()
        data ++= d
      }

      ssc
    })

    val stopThread = createStopThread(ssc, 2000)

    stopThread.start()

    ssc.start()

    ssc.awaitTermination()

    data.size should be(10)
    data should be(ListBuffer("LINE 001 - 20161201", "LINE 002 - 20161201", "LINE 003 - 20161201", "LINE 004 - 20161201", "LINE 005 - 20161201",
      "LINE 001 - 20161202", "LINE 002 - 20161202", "LINE 003 - 20161202", "LINE 004 - 20161202", "LINE 005 - 20161202"))
  }

  it should "process files with download failures but not enough retries" in {
    FileUtils.deleteQuietly(new File(checkPointDirectory.getPath))

    val files = Seq(DownloadFile("/files/example_20161201.txt", dt.parse("2016-12-01 00:00:00")),
      DownloadFile("/files/example_20161202.txt", dt.parse("2016-12-02 00:00:00")))
    val date = dt.parse("2016-01-01 00:00:00")

    var data: ListBuffer[String] = ListBuffer();

    val ssc = StreamingContext.getOrCreate(checkPointDirectory.getPath, () => {
      val ssc = new StreamingContext(sc, batchTime)

      val ds = ssc.createDownloadStream(DownloaderFactoryHelper.createDownloaderFactory(files, downloadBadTries = 100), Map(), Option(DownloadFileRange(date)), maxRetries = 2)

      ds.checkpoint(checkpointTime)

      ssc.checkpoint(checkPointDirectory.getPath)

      ds.foreachRDD { rdd =>
        val d = rdd.collect()
        data ++= d
      }

      ssc
    })

    val stopThread = createStopThread(ssc, 2000)

    stopThread.start()

    ssc.start()

    intercept[org.apache.spark.SparkException] {
      ssc.awaitTermination()
    }

    while (stopThread.isAlive) Thread.sleep(100)

    data.size should be(0)
  }

  it should "process some batches" in {
    FileUtils.deleteQuietly(new File(checkPointDirectory.getPath))

    val files = Seq(DownloadFile("/files/example_20161201.txt", dt.parse("2016-12-01 00:00:00")),
      DownloadFile("/files/example_20161202.txt", dt.parse("2016-12-02 00:00:00")))
    val date = dt.parse("2016-12-01 00:00:00")

    var data: ListBuffer[String] = ListBuffer();

    val ssc = StreamingContext.getOrCreate(checkPointDirectory.getPath, () => {
      val ssc = new StreamingContext(sc, batchTime)

      val ds = ssc.createDownloadStream(DownloaderFactoryHelper.createDownloaderFactory(files, splitInside = false), Map(), Option(DownloadFileRange(date)))

      ds.checkpoint(checkpointTime)

      ssc.checkpoint(checkPointDirectory.getPath)

      ds.foreachRDD { rdd =>
        val d = rdd.collect()
        data ++= d
      }

      ssc
    })

    val stopThread = createStopThread(ssc, 2000)

    stopThread.start()

    ssc.start()

    ssc.awaitTermination()

    data.size should be(10)
    data should be(ListBuffer("LINE 001 - 20161201", "LINE 002 - 20161201", "LINE 003 - 20161201", "LINE 004 - 20161201", "LINE 005 - 20161201",
      "LINE 001 - 20161202", "LINE 002 - 20161202", "LINE 003 - 20161202", "LINE 004 - 20161202", "LINE 005 - 20161202"))
  }

  it should "process some batches with checkpoint" in {
    FileUtils.deleteQuietly(new File(checkPointDirectory.getPath))

    val date = dt.parse("2016-12-01 00:00:00")

    var data: ListBuffer[String] = ListBuffer();

    var ssc = StreamingContext.getOrCreate(checkPointDirectory.getPath, () => {
      val ssc = new StreamingContext(sc, batchTime)

      val ds = ssc.createDownloadStream(DownloaderFactoryHelper.createDownloaderFactory(Seq(DownloadFile("/files/example_20161201.txt", dt.parse("2016-12-01 00:00:00")))), Map(), Option(DownloadFileRange(date)))

      ds.checkpoint(checkpointTime)

      ssc.checkpoint(checkPointDirectory.getPath)

      ds.foreachRDD { rdd =>
        val d = rdd.collect()
        data ++= d
      }

      ssc
    })

    var stopThread = createStopThread(ssc, 2000)

    stopThread.start()

    ssc.start()

    ssc.awaitTermination()

    data.size should be(5)

    ssc = StreamingContext.getOrCreate(checkPointDirectory.getPath, () => {
      val ssc = new StreamingContext(sc, batchTime)

      val ds = ssc.createDownloadStream(DownloaderFactoryHelper.createDownloaderFactory(Seq(DownloadFile("/files/example_20161201.txt", dt.parse("2016-12-01 00:00:00")),
        DownloadFile("/files/example_20161202.txt", dt.parse("2016-12-02 00:00:00")))), Map(), Option(DownloadFileRange(date)))

      ds.checkpoint(checkpointTime)

      ssc.checkpoint(checkPointDirectory.getPath)

      ds.foreachRDD { rdd =>
        val d = rdd.collect()
        data ++= d
      }

      ssc
    })

    stopThread = createStopThread(ssc, 2000)

    stopThread.start()

    ssc.start()

    ssc.awaitTermination()

    data.size should be(5)
    data should be(ListBuffer("LINE 001 - 20161201", "LINE 002 - 20161201", "LINE 003 - 20161201", "LINE 004 - 20161201", "LINE 005 - 20161201"))
  }

  it should "process files with same dates" in {
    FileUtils.deleteQuietly(new File(checkPointDirectory.getPath))

    val files = Seq(DownloadFile("/files/example_20161201_1.txt", dt.parse("2016-12-01 00:00:00")),
      DownloadFile("/files/example_20161201_2.txt", dt.parse("2016-12-01 00:00:00")))
    val date = dt.parse("2016-01-01 00:00:00")

    var data: ListBuffer[String] = ListBuffer();

    val ssc = StreamingContext.getOrCreate(checkPointDirectory.getPath, () => {
      val ssc = new StreamingContext(sc, batchTime)

      val ds = ssc.createDownloadStream(DownloaderFactoryHelper.createDownloaderFactory(files), Map(), Option(DownloadFileRange(date)))

      ds.checkpoint(checkpointTime)

      ssc.checkpoint(checkPointDirectory.getPath)

      ds.foreachRDD { rdd =>
        val d = rdd.collect()
        data ++= d
      }

      ssc
    })

    val stopThread = createStopThread(ssc, 2000)

    stopThread.start()

    ssc.start()

    ssc.awaitTermination()

    data.size should be(10)
    data should be(ListBuffer("LINE 001 - 20161201_1", "LINE 002 - 20161201_1", "LINE 003 - 20161201_1", "LINE 004 - 20161201_1", "LINE 005 - 20161201_1",
      "LINE 001 - 20161201_2", "LINE 002 - 20161201_2", "LINE 003 - 20161201_2", "LINE 004 - 20161201_2", "LINE 005 - 20161201_2"))
  }

  it should "process only one file because DateDownloadStreamFileRange" in {
    FileUtils.deleteQuietly(new File(checkPointDirectory.getPath))

    val files = Seq(DownloadFile("/files/example_20161201.txt", dt.parse("2016-12-01 00:00:00")),
      DownloadFile("/files/example_20161202.txt", dt.parse("2016-12-02 00:00:00")))
    val date = dt.parse("2016-12-02 00:00:00")

    var data: ListBuffer[String] = ListBuffer();

    val ssc = StreamingContext.getOrCreate(checkPointDirectory.getPath, () => {
      val ssc = new StreamingContext(sc, batchTime)

      val ds = ssc.createDownloadStream(DownloaderFactoryHelper.createDownloaderFactory(files), Map(), Option(DownloadFileRange(date)))

      ds.checkpoint(checkpointTime)

      ssc.checkpoint(checkPointDirectory.getPath)

      ds.foreachRDD { rdd =>
        val d = rdd.collect()
        data ++= d
      }

      ssc
    })

    val stopThread = createStopThread(ssc, 2000)

    stopThread.start()

    ssc.start()

    ssc.awaitTermination()

    data.size should be(5)
    data should be(ListBuffer("LINE 001 - 20161202", "LINE 002 - 20161202", "LINE 003 - 20161202", "LINE 004 - 20161202", "LINE 005 - 20161202"))
  }

  it should "process only one file because DateFilesDownloadStreamFileRange" in {
    FileUtils.deleteQuietly(new File(checkPointDirectory.getPath))

    val files = Seq(DownloadFile("/files/sampleFile_20161011.txt", dt.parse("2016-10-11 00:00:00")),
      DownloadFile("/files/example_20161201_2.txt", dt.parse("2016-12-01 00:00:00")),
      DownloadFile("/files/example_20161201_1.txt", dt.parse("2016-12-01 00:00:00")))
    val date = dt.parse("2016-12-01 00:00:00")
    var data: ListBuffer[String] = ListBuffer();

    val ssc = StreamingContext.getOrCreate(checkPointDirectory.getPath, () => {
      val ssc = new StreamingContext(sc, batchTime)

      val ds = ssc.createDownloadStream(DownloaderFactoryHelper.createDownloaderFactory(files, splitInside = true), Map(), Option(DownloadFileRange(date, "/files/example_20161201_1.txt")))

      ds.checkpoint(checkpointTime)

      ssc.checkpoint(checkPointDirectory.getPath)

      ds.foreachRDD { rdd =>
        val d = rdd.collect()
        data ++= d
      }

      ssc
    })

    val stopThread = createStopThread(ssc, 2000)

    stopThread.start()

    ssc.start()

    ssc.awaitTermination()

    data.size should be(5)
    data should be(ListBuffer("LINE 001 - 20161201_2", "LINE 002 - 20161201_2", "LINE 003 - 20161201_2", "LINE 004 - 20161201_2", "LINE 005 - 20161201_2"))
  }
}
