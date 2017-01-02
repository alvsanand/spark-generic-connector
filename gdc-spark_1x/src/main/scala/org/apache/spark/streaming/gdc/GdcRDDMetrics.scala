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
  * Created by alvsanand on 2/01/17.
  */
private[gdc]
class GdcRDDMetrics(private val context: TaskContext) {
  private val counter = new AtomicLong()

  private val inputMetrics = context.taskMetrics.getInputMetricsForReadMethod(DataReadMethod.Hadoop)

  if (inputMetrics.bytesReadCallback.isEmpty) {
    val bytesReadCallback = inputMetrics.bytesReadCallback.orElse {
      Some(() => counter.get().toLong)
    }

    inputMetrics.setBytesReadCallback(bytesReadCallback)
  }

  def updateBytesRead(numBytes: Long = 0): Unit = {
    counter.addAndGet(numBytes)
  }

  def updateRecordRead(numRecords: Long = 1): Unit = {
    inputMetrics.incRecordsRead(numRecords)
  }
}
