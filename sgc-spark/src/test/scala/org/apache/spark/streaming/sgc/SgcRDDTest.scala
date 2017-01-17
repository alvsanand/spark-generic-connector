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

package org.apache.spark.streaming.sgc

import java.util.Date

import es.alvsanand.sgc.core.connector.{SgcConnectorParameters, SgcSlot}
import es.alvsanand.sgc.core.util.{SgcConnectorFactoryHelper, SparkTest}

class SgcRDDTest extends SparkTest {
  it should "return one partition" in {
    val slot = SgcSlot("/files/example.txt")

    val rdd = new SgcRDD(sc, Array(slot), SgcConnectorFactoryHelper.createFactory(Seq
    (slot)), SgcConnectorParameters())

    rdd.partitions should be(Array(new SgcRDDPartition(slot, 0)))
  }

  it should "return three partition" in {
    val slots = Array(
      SgcSlot("/files/example.txt"),
      SgcSlot("/files/example_20161201.txt"),
      SgcSlot("/files/example_20161202.txt"))

    val rdd = new SgcRDD(sc, slots, SgcConnectorFactoryHelper.createFactory(slots),
      SgcConnectorParameters())
    val partitions = rdd.partitions

    partitions.size should be(3)
    partitions(0).asInstanceOf[SgcRDDPartition[SgcSlot]].slot should be(slots(0))
    partitions(0).asInstanceOf[SgcRDDPartition[SgcSlot]].index should be(0)
    partitions(1).asInstanceOf[SgcRDDPartition[SgcSlot]].slot should be(slots(1))
    partitions(1).asInstanceOf[SgcRDDPartition[SgcSlot]].index should be(1)
    partitions(2).asInstanceOf[SgcRDDPartition[SgcSlot]].slot should be(slots(2))
    partitions(2).asInstanceOf[SgcRDDPartition[SgcSlot]].index should be(2)
  }

  it should "test simple File" in {
    val slot = SgcSlot("/files/example.txt")

    val rdd = new SgcRDD(sc, Array(slot), SgcConnectorFactoryHelper.createFactory(Seq
    (slot)), SgcConnectorParameters())

    rdd.count() should be(5)
    rdd.collect() should be(Array("LINE 001", "LINE 002", "LINE 003", "LINE 004", "LINE 005"))
  }

  it should "test multiple Files" in {
    val slots = Array(
      SgcSlot("/files/example_20161201.txt"),
      SgcSlot("/files/example_20161202.txt"))

    val rdd = new SgcRDD(sc, slots, SgcConnectorFactoryHelper.createFactory(slots),
      SgcConnectorParameters())

    rdd.count() should be(10)
    rdd.collect() should be(Array("LINE 001 - 20161201", "LINE 002 - 20161201", "LINE 003 - " +
      "20161201", "LINE 004 - " +
      "20161201", "LINE 005 - 20161201",
      "LINE 001 - 20161202", "LINE 002 - 20161202", "LINE 003 - 20161202", "LINE 004 - 20161202",
      "LINE 005 - " +
        "20161202"))
  }

  it should "test simple File with enough retries" in {
    val slot = SgcSlot("/files/example.txt")

    val rdd = new SgcRDD(sc, Array(slot), SgcConnectorFactoryHelper.createFactory(Seq
    (slot),
      fetchBadTries = 2), SgcConnectorParameters(), maxRetries = 2)

    rdd.count() should be(5)
    rdd.collect() should be(Array("LINE 001", "LINE 002", "LINE 003", "LINE 004", "LINE 005"))
  }

  it should "test simple File with not enough retries" in {
    val slot = SgcSlot("/files/example.txt")

    intercept[org.apache.spark.SparkException] {
      new SgcRDD(sc, Array(slot), SgcConnectorFactoryHelper.createFactory(Seq(slot),
        fetchBadTries =
        2), SgcConnectorParameters(), maxRetries = 1).collect()
    }
  }

  it should "test bad File" in {
    val slot = SgcSlot("BAD_FILE")

    intercept[org.apache.spark.SparkException] {
      new SgcRDD(sc, Array(slot), SgcConnectorFactoryHelper.createFactory(Seq(slot)),
        SgcConnectorParameters()).collect()
    }
  }

  it should "test simple GZ File" in {
    val slot = SgcSlot("/files/example.txt.gz")

    val rdd = new SgcRDD(sc, Array(slot), SgcConnectorFactoryHelper.createFactory(Seq
    (slot)), SgcConnectorParameters())

    rdd.count() should be(5)
    rdd.collect() should be(Array("LINE 001", "LINE 002", "LINE 003", "LINE 004", "LINE 005"))
  }
}
