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

package org.apache.spark.streaming.sgdc

import java.io.File

import es.alvsanand.sgdc.core.downloader.{SgdcDownloaderParameters, SgdcSlot}
import es.alvsanand.sgdc.core.util
import es.alvsanand.sgdc.core.util.{SgdcDownloaderFactoryHelper, IOUtils, SparkTest}
import org.apache.commons.io.FileUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

import scala.collection.mutable.ListBuffer

class SgdcInputDStreamTest extends SparkTest {

  val checkPointDirectory = IOUtils.createTempDirectory()
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

  it should "process slots" in {
    FileUtils.deleteQuietly(new File(checkPointDirectory.getPath))

    val slots = Seq(SgdcSlot("/files/example_20161201.txt", dt.parse("2016-12-01 00:00:00")),
      SgdcSlot("/files/example_20161202.txt", dt.parse("2016-12-02 00:00:00")))
    val date = dt.parse("2016-01-01 00:00:00")

    var data: ListBuffer[String] = ListBuffer();

    val ssc = StreamingContext.getOrCreate(checkPointDirectory.getPath, () => {
      val ssc = new StreamingContext(sc, batchTime)

      val ds = ssc.createSgdcInputDStream(SgdcDownloaderFactoryHelper.createDateFactory(slots)
        , SgdcDownloaderParameters(), Option
      (SgdcRange(date)))

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
    data should be(ListBuffer("LINE 001 - 20161201", "LINE 002 - 20161201", "LINE 003 - " +
      "20161201", "LINE 004 - " +
      "20161201", "LINE 005 - 20161201",
      "LINE 001 - 20161202", "LINE 002 - 20161202", "LINE 003 - 20161202", "LINE 004 - 20161202",
      "LINE 005 - " +
        "20161202"))
  }

  it should "process slots with list failures" in {
    FileUtils.deleteQuietly(new File(checkPointDirectory.getPath))

    val slots = Seq(SgdcSlot("/files/example_20161201.txt", dt.parse("2016-12-01 00:00:00")),
      SgdcSlot("/files/example_20161202.txt", dt.parse("2016-12-02 00:00:00")))
    val date = dt.parse("2016-01-01 00:00:00")

    var data: ListBuffer[String] = ListBuffer();

    val ssc = StreamingContext.getOrCreate(checkPointDirectory.getPath, () => {
      val ssc = new StreamingContext(sc, batchTime)

      val ds = ssc.createSgdcInputDStream(SgdcDownloaderFactoryHelper.createDateFactory(slots,
        listBadTries = 1), SgdcDownloaderParameters(), Option(SgdcRange(date)))

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
    data should be(ListBuffer("LINE 001 - 20161201", "LINE 002 - 20161201", "LINE 003 - " +
      "20161201", "LINE 004 - " +
      "20161201", "LINE 005 - 20161201",
      "LINE 001 - 20161202", "LINE 002 - 20161202", "LINE 003 - 20161202", "LINE 004 - 20161202",
      "LINE 005 - " +
        "20161202"))
  }

  it should "process slots with list failures but not enough retries" in {
    FileUtils.deleteQuietly(new File(checkPointDirectory.getPath))

    val slots = Seq(SgdcSlot("/files/example_20161201.txt", dt.parse("2016-12-01 00:00:00")),
      SgdcSlot("/files/example_20161202.txt", dt.parse("2016-12-02 00:00:00")))
    val date = dt.parse("2016-01-01 00:00:00")

    var data: ListBuffer[String] = ListBuffer();

    val ssc = StreamingContext.getOrCreate(checkPointDirectory.getPath, () => {
      val ssc = new StreamingContext(sc, batchTime)

      val ds = ssc.createSgdcInputDStream(SgdcDownloaderFactoryHelper.createDateFactory(slots,
        listBadTries = 100),
        SgdcDownloaderParameters(), Option(SgdcRange(date)), maxRetries = 1)

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

  it should "process slots with download failures" in {
    FileUtils.deleteQuietly(new File(checkPointDirectory.getPath))

    val slots = Seq(SgdcSlot("/files/example_20161201.txt", dt.parse("2016-12-01 00:00:00")),
      SgdcSlot("/files/example_20161202.txt", dt.parse("2016-12-02 00:00:00")))
    val date = dt.parse("2016-01-01 00:00:00")

    var data: ListBuffer[String] = ListBuffer();

    val ssc = StreamingContext.getOrCreate(checkPointDirectory.getPath, () => {
      val ssc = new StreamingContext(sc, batchTime)

      val ds = ssc.createSgdcInputDStream(SgdcDownloaderFactoryHelper.createDateFactory(slots,
        downloadBadTries = 1),
        SgdcDownloaderParameters(), Option(SgdcRange(date)), maxRetries = 2)

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
    data should be(ListBuffer("LINE 001 - 20161201", "LINE 002 - 20161201", "LINE 003 - " +
      "20161201", "LINE 004 - " +
      "20161201", "LINE 005 - 20161201",
      "LINE 001 - 20161202", "LINE 002 - 20161202", "LINE 003 - 20161202", "LINE 004 - 20161202",
      "LINE 005 - " +
        "20161202"))
  }

  it should "process slots with download failures but not enough retries" in {
    FileUtils.deleteQuietly(new File(checkPointDirectory.getPath))

    val slots = Seq(SgdcSlot("/files/example_20161201.txt", dt.parse("2016-12-01 00:00:00")),
      SgdcSlot("/files/example_20161202.txt", dt.parse("2016-12-02 00:00:00")))
    val date = dt.parse("2016-01-01 00:00:00")

    var data: ListBuffer[String] = ListBuffer();

    val ssc = StreamingContext.getOrCreate(checkPointDirectory.getPath, () => {
      val ssc = new StreamingContext(sc, batchTime)

      val ds = ssc.createSgdcInputDStream(SgdcDownloaderFactoryHelper.createDateFactory(slots,
        downloadBadTries =
        100), SgdcDownloaderParameters(), Option(SgdcRange(date)), maxRetries = 2)

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

    val slots = Seq(SgdcSlot("/files/example_20161201.txt", dt.parse("2016-12-01 00:00:00")),
      SgdcSlot("/files/example_20161202.txt", dt.parse("2016-12-02 00:00:00")))
    val date = dt.parse("2016-12-01 00:00:00")

    var data: ListBuffer[String] = ListBuffer();

    val ssc = StreamingContext.getOrCreate(checkPointDirectory.getPath, () => {
      val ssc = new StreamingContext(sc, batchTime)

      val ds = ssc.createSgdcInputDStream(SgdcDownloaderFactoryHelper.createDateFactory(slots,
        splitInside = false),
        SgdcDownloaderParameters(), Option(SgdcRange(date)))

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
    data should be(ListBuffer("LINE 001 - 20161201", "LINE 002 - 20161201", "LINE 003 - " +
      "20161201", "LINE 004 - " +
      "20161201", "LINE 005 - 20161201",
      "LINE 001 - 20161202", "LINE 002 - 20161202", "LINE 003 - 20161202", "LINE 004 - 20161202",
      "LINE 005 - " +
        "20161202"))
  }

  it should "process some batches with checkpoint" in {
    FileUtils.deleteQuietly(new File(checkPointDirectory.getPath))

    val date = dt.parse("2016-12-01 00:00:00")

    var data: ListBuffer[String] = ListBuffer();

    var ssc = StreamingContext.getOrCreate(checkPointDirectory.getPath, () => {
      val ssc = new StreamingContext(sc, batchTime)

      val ds = ssc.createSgdcInputDStream(SgdcDownloaderFactoryHelper.createDateFactory(Seq
      (SgdcSlot
      ("/files/example_20161201.txt", dt.parse("2016-12-01 00:00:00")))), SgdcDownloaderParameters(), Option(SgdcRange
      (date)))

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

      val ds = ssc.createSgdcInputDStream(SgdcDownloaderFactoryHelper.createDateFactory(Seq
      (SgdcSlot
      ("/files/example_20161201.txt", dt.parse("2016-12-01 00:00:00")),
        SgdcSlot("/files/example_20161202.txt", dt.parse("2016-12-02 00:00:00")))), SgdcDownloaderParameters(), Option
      (SgdcRange(date)))

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
    data should be(ListBuffer("LINE 001 - 20161201", "LINE 002 - 20161201", "LINE 003 - " +
      "20161201", "LINE 004 - " +
      "20161201", "LINE 005 - 20161201"))
  }

  it should "process slots with same dates" in {
    FileUtils.deleteQuietly(new File(checkPointDirectory.getPath))

    val slots = Seq(SgdcSlot("/files/example_20161201_1.txt", dt.parse("2016-12-01 00:00:00")),
      SgdcSlot("/files/example_20161201_2.txt", dt.parse("2016-12-01 00:00:00")))
    val date = dt.parse("2016-01-01 00:00:00")

    var data: ListBuffer[String] = ListBuffer();

    val ssc = StreamingContext.getOrCreate(checkPointDirectory.getPath, () => {
      val ssc = new StreamingContext(sc, batchTime)

      val ds = ssc.createSgdcInputDStream(SgdcDownloaderFactoryHelper.createDateFactory(slots)
        , SgdcDownloaderParameters(), Option
      (SgdcRange(date)))

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
    data should be(ListBuffer("LINE 001 - 20161201_1", "LINE 002 - 20161201_1", "LINE 003 - " +
      "20161201_1", "LINE 004 - " +
      "20161201_1", "LINE 005 - 20161201_1",
      "LINE 001 - 20161201_2", "LINE 002 - 20161201_2", "LINE 003 - 20161201_2", "LINE 004 - " +
        "20161201_2", "LINE 005 -" +
        " 20161201_2"))
  }

  it should "process only one name because SgdcDateRange" in {
    FileUtils.deleteQuietly(new File(checkPointDirectory.getPath))

    val slots = Seq(SgdcSlot("/files/example_20161201.txt", dt.parse("2016-12-01 00:00:00")),
      SgdcSlot("/files/example_20161202.txt", dt.parse("2016-12-02 00:00:00")))
    val date = dt.parse("2016-12-02 00:00:00")

    var data: ListBuffer[String] = ListBuffer();

    val ssc = StreamingContext.getOrCreate(checkPointDirectory.getPath, () => {
      val ssc = new StreamingContext(sc, batchTime)

      val ds = ssc.createSgdcInputDStream(SgdcDownloaderFactoryHelper.createDateFactory(slots)
        , SgdcDownloaderParameters(), Option
      (SgdcRange(date)))

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
    data should be(ListBuffer("LINE 001 - 20161202", "LINE 002 - 20161202", "LINE 003 - " +
      "20161202", "LINE 004 - " +
      "20161202", "LINE 005 - 20161202"))
  }

  it should "process only one name because SgdcDateSlotsRange" in {
    FileUtils.deleteQuietly(new File(checkPointDirectory.getPath))

    val slots = Seq(SgdcSlot("/files/sampleFile_20161011.txt", dt.parse("2016-10-11 00:00:00")),
      SgdcSlot("/files/example_20161201_2.txt", dt.parse("2016-12-01 00:00:00")),
      SgdcSlot("/files/example_20161201_1.txt", dt.parse("2016-12-01 00:00:00")))
    val date = dt.parse("2016-12-01 00:00:00")
    var data: ListBuffer[String] = ListBuffer();

    val ssc = StreamingContext.getOrCreate(checkPointDirectory.getPath, () => {
      val ssc = new StreamingContext(sc, batchTime)

      val ds = ssc.createSgdcInputDStream(SgdcDownloaderFactoryHelper.createDateFactory(slots,
        splitInside = true),
        SgdcDownloaderParameters(), Option(SgdcRange(date, Seq("/files/example_20161201_1.txt"))))

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
    data should be(ListBuffer("LINE 001 - 20161201_2", "LINE 002 - 20161201_2", "LINE 003 - " +
      "20161201_2", "LINE 004 - " +
      "20161201_2", "LINE 005 - 20161201_2"))
  }
}
