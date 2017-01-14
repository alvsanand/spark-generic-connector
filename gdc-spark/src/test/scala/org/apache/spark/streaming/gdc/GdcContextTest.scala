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

import es.alvsanand.gdc.core.downloader.{GdcDownloaderParameters, GdcSlot}
import es.alvsanand.gdc.core.util.{GdcDownloaderFactoryHelper, SparkTest}

class GdcContextTest extends SparkTest {

  it should "Return all" in {
    val slots = Array(
      GdcSlot("/files/example.txt"),
      GdcSlot("/files/example_20161201.txt", dt.parse("2016-10-12 00:00:00")),
      GdcSlot("/files/example_20161202.txt", dt.parse("2016-10-13 00:00:00")))

    val rdd = sc.createGdcRDD(GdcDownloaderFactoryHelper.createFactory(slots),
      GdcDownloaderParameters())
    val partitions = rdd.partitions

    partitions.size should be(3)
    partitions(0).asInstanceOf[GdcRDDPartition[GdcSlot]].slot should be(slots(0))
    partitions(0).asInstanceOf[GdcRDDPartition[GdcSlot]].index should be(0)
    partitions(1).asInstanceOf[GdcRDDPartition[GdcSlot]].slot should be(slots(1))
    partitions(1).asInstanceOf[GdcRDDPartition[GdcSlot]].index should be(1)
    partitions(2).asInstanceOf[GdcRDDPartition[GdcSlot]].slot should be(slots(2))
    partitions(2).asInstanceOf[GdcRDDPartition[GdcSlot]].index should be(2)
  }

  it should "Return only first with Previous filter" in {
    val slots = Array(
      GdcSlot("/files/example.txt"),
      GdcSlot("/files/example_20161201.txt", dt.parse("2016-10-12 00:00:00")),
      GdcSlot("/files/example_20161202.txt", dt.parse("2016-10-13 00:00:00")))

    val rdd = sc.createPreviousFilteredGdcRDD(GdcDownloaderFactoryHelper.createFactory(slots),
      GdcDownloaderParameters(), Array(GdcSlot("/files/example.txt")))
    val partitions = rdd.partitions

    partitions.size should be(2)
    partitions(0).asInstanceOf[GdcRDDPartition[GdcSlot]].slot should be(slots(1))
    partitions(0).asInstanceOf[GdcRDDPartition[GdcSlot]].index should be(0)
    partitions(1).asInstanceOf[GdcRDDPartition[GdcSlot]].slot should be(slots(2))
    partitions(1).asInstanceOf[GdcRDDPartition[GdcSlot]].index should be(1)
  }

  it should "Return all slots with date filter A" in {
    val slots = Array(
      GdcSlot("/files/example.txt", dt.parse("2016-10-11 00:00:00")),
      GdcSlot("/files/example_20161201.txt", dt.parse("2016-10-12 00:00:00")),
      GdcSlot("/files/example_20161202.txt", dt.parse("2016-10-13 00:00:00")))

    val rdd = sc.createDateFilteredGdcRDD(GdcDownloaderFactoryHelper.createDateFactory(slots),
      GdcDownloaderParameters(), Option(dt.parse
    ("2016-10-11 00:00:00")), Option(dt.parse("2016-10-13 00:00:01")))
    val partitions = rdd.partitions

    partitions.size should be(3)
    partitions(0).asInstanceOf[GdcRDDPartition[GdcSlot]].slot should be(slots(0))
    partitions(0).asInstanceOf[GdcRDDPartition[GdcSlot]].index should be(0)
    partitions(1).asInstanceOf[GdcRDDPartition[GdcSlot]].slot should be(slots(1))
    partitions(1).asInstanceOf[GdcRDDPartition[GdcSlot]].index should be(1)
    partitions(2).asInstanceOf[GdcRDDPartition[GdcSlot]].slot should be(slots(2))
    partitions(2).asInstanceOf[GdcRDDPartition[GdcSlot]].index should be(2)
  }

  it should "Return 2 slots with date filter B" in {
    val slots = Array(
      GdcSlot("/files/example.txt", dt.parse("2016-10-11 00:00:00")),
      GdcSlot("/files/example_20161201.txt", dt.parse("2016-10-12 00:00:00")),
      GdcSlot("/files/example_20161202.txt", dt.parse("2016-10-13 00:00:00")))

    val rdd = sc.createDateFilteredGdcRDD(GdcDownloaderFactoryHelper.createDateFactory(slots),
      GdcDownloaderParameters(), None, Option(dt
      .parse("2016-10-12 00:00:01")))
    val partitions = rdd.partitions

    partitions.size should be(2)
    partitions(0).asInstanceOf[GdcRDDPartition[GdcSlot]].slot should be(slots(0))
    partitions(0).asInstanceOf[GdcRDDPartition[GdcSlot]].index should be(0)
    partitions(1).asInstanceOf[GdcRDDPartition[GdcSlot]].slot should be(slots(1))
    partitions(1).asInstanceOf[GdcRDDPartition[GdcSlot]].index should be(1)
  }

  it should "Return 2 slots with date filter C" in {
    val slots = Array(
      GdcSlot("/files/example.txt", dt.parse("2016-10-11 00:00:00")),
      GdcSlot("/files/example_20161201.txt", dt.parse("2016-10-12 00:00:00")),
      GdcSlot("/files/example_20161202.txt", dt.parse("2016-10-13 00:00:00")))

    val rdd = sc.createDateFilteredGdcRDD(GdcDownloaderFactoryHelper.createDateFactory(slots),
      GdcDownloaderParameters(), Option(dt.parse
    ("2016-10-12 00:00:00")), None)
    val partitions = rdd.partitions

    partitions.size should be(2)
    partitions(0).asInstanceOf[GdcRDDPartition[GdcSlot]].slot should be(slots(1))
    partitions(0).asInstanceOf[GdcRDDPartition[GdcSlot]].index should be(0)
    partitions(1).asInstanceOf[GdcRDDPartition[GdcSlot]].slot should be(slots(2))
    partitions(1).asInstanceOf[GdcRDDPartition[GdcSlot]].index should be(1)
  }

  it should "Return no slots with date filter" in {
    val slots = Array(
      GdcSlot("/files/example.txt", dt.parse("2016-10-11 00:00:00")),
      GdcSlot("/files/example_20161201.txt", dt.parse("2016-10-12 00:00:00")),
      GdcSlot("/files/example_20161202.txt", dt.parse("2016-10-13 00:00:00")))

    val rdd = sc.createDateFilteredGdcRDD(GdcDownloaderFactoryHelper.createDateFactory(slots),
      GdcDownloaderParameters(), None, Option(dt
      .parse("2016-10-10 23:59:59")))
    val partitions = rdd.partitions

    partitions.size should be(0)
  }
}
