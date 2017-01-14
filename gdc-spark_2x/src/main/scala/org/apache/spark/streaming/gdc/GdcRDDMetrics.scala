/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional logInformation regarding copyright ownership.
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

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.TaskContext
import org.apache.spark.executor.DataReadMethod

/**
  * This class is the responsible of write metrics to the Spark TaskContext.
  */
class GdcRDDMetrics(private val context: TaskContext) {
  private val counter = new AtomicLong()

  private val inputMetrics = context.taskMetrics.inputMetrics

  private val existingBytesRead = inputMetrics.bytesRead

  /**
    * Updated the bytes read
    * @param numBytes The number of read bytes
    */
  def updateBytesRead(numBytes: Long = 0): Unit = {
    counter.addAndGet(numBytes)

    inputMetrics.setBytesRead(existingBytesRead + counter.get().toLong)
  }

  /**
    *  Updated the number of read slots
    * @param numRecords The number of read slots
    */
  def updateRecordRead(numRecords: Long = 1): Unit = {
    inputMetrics.incRecordsRead(numRecords)
  }
}
