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

package org.apache.spark.streaming.sgc

import java.io.{IOException, ObjectInputStream}

import es.alvsanand.sgc.core.connector._
import es.alvsanand.sgc.core.util.Retry
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.sgc.SgcRange.{SgcDateSlotsRange, SgcDateRange}
import org.apache.spark.streaming.scheduler.StreamInputInfo
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.util.Utils

import scala.collection.mutable
import scala.collection.mutable.HashMap
import scala.reflect.ClassTag
import scala.util.{Failure, Success}


/**
  * This class represents an input stream that monitors the slots returned by a
  * es.alvsanand.sgc.core.connector.SgcConnector. When a new slots comes, it creates a
  * org.apache.spark.streaming.sgc.SgcRDD. The way it works as follows.
  *
  * At each batch interval, the es.alvsanand.sgc.core.connector.SgcConnector list all its
  * available slots and detects if there are new slots which will be for that batch. For this
  * purpose, this class remembers the information about the slots selected in past batches for
  * a certain duration
  *
  * This makes some assumptions from the other system that
  * es.alvsanand.sgc.core.connector.SgcConnector is monitoring.
  *
  *  - The clock of the name system is assumed to synchronized with the clock of the machine running
  *    the streaming app.
  *  - The es.alvsanand.sgc.core.connector.SgcDateSlot returned by the
  *  es.alvsanand.sgc.core.connector.SgcConnector should return a date because if not it will
  *  be sorted by name.
 *
  * @param ssc_ The StreamingContext
  * @param sgcConnectorFactory The SgcConnectorFactory used to create the
  *                             es.alvsanand.sgc.core.connector.SgcConnector.
  * @param parameters The parameters of the es.alvsanand.sgc.core.connector.SgcConnector.
  * @param fromSgcRange The range from where the SgcInputDStream will begin.
  * @param charset The java.nio.charset.Charset name of the slots that are going to be
  *                fetched.
  * @param maxRetries  The maximum number times that an operation of a
  *                   es.alvsanand.sgc.core.connector.SgcConnector is going to be repeated
  *                   in case of failure.
  * @tparam A The type of es.alvsanand.sgc.core.connector.SgcSlot
  * @tparam B The type of es.alvsanand.sgc.core.connector.SgcConnectorParameters
  */
private[sgc]
final class SgcInputDStream[A <: SgcDateSlot: ClassTag, B <: SgcConnectorParameters: ClassTag](
                                                  ssc_ : StreamingContext,
                                                  sgcConnectorFactory: SgcConnectorFactory[A, B],
                                                  parameters: B,
                                                  fromSgcRange:
                                                  Option[SgcRange] = None,
                                                  charset: String = "UTF-8",
                                                  maxRetries: Int = 3)
  extends InputDStream[String](ssc_) {
  private[streaming] override val checkpointData = new SgcInputDStreamCheckpointData
  @transient private var _sgcConnector: SgcConnector[A, B] = null

  // Remember the last 100 batches
  remember(slideDuration * 100)

  // Map with the slots batched in a certain time
  @transient private var batchTimeToSelectedFiles = new mutable.HashMap[Time, Array[A]]

  // Set of slots that were selected in the remembered batches
  @transient private var recentlySelectedFiles = new mutable.HashSet[String]()

  /** See [[https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.streaming.dst
    * ream.InputDStream]]
    */
  override def compute(validTime: Time): Option[RDD[String]] = {
    Retry(maxRetries) {
      sgcConnector.list()
    } match {
      case Success(list) => {
        val newSlots: Array[A] = list.flatMap(f => isNewSlot(f))
          .sortWith { case (a, b) => a.compare(b) < 0 }
          .toArray

        if (newSlots.length == 0) {
          None
        }
        else {
          logInfo(s"Detected new Files[${newSlots.mkString(",")}] to process")

          batchTimeToSelectedFiles.synchronized {
            batchTimeToSelectedFiles += ((validTime, newSlots))
          }
          recentlySelectedFiles ++= newSlots.map(_.name)

          // One at a time
          val rdd = slotsToRDD(newSlots)

          val metadata = Map(
            "newSlots" -> newSlots,
            "sgcConnectorParameters" -> parameters,
            "fromSgcRange" -> fromSgcRange,
            StreamInputInfo.METADATA_KEY_DESCRIPTION -> newSlots.mkString("\n"))
          val inputInfo = StreamInputInfo(id, rdd.count, metadata)
          ssc.scheduler.inputInfoTracker.reportInfo(validTime, inputInfo)

          Some(rdd)
        }
      }
      case Failure(e) => {
        val msg = s"Error computing SgcInputDStream after $maxRetries tries"
        logError(msg)

        throw new org.apache.spark.SparkException(msg, e)
      }
    }
  }

  /**
    * Internal method that instantiate a es.alvsanand.sgc.core.connector.SgcConnector.
    * @return
    */
  private def sgcConnector = {
    if (_sgcConnector == null) {
      _sgcConnector = sgcConnectorFactory.get(parameters)
    }
    _sgcConnector
  }

  /**
    * Internal method that check if a es.alvsanand.sgc.core.connector.SgcSlot has not been
    * processed before
 *
    * @param slot The name to be checked
    * @return Some(name) if it is new, None otherwise.
    */
  private def isNewSlot(slot: A): Option[A] = {
    // Reject name if it is old
    val validRange: Boolean = fromSgcRange match {
      case Some(r: SgcDateRange) =>
        (r.date == null ||
          (slot.date!=null
            && r.date.getTime <= slot.date.getTime)
          )
      case Some(r: SgcDateSlotsRange) =>
        ((r.slots == null || !r.slots.contains(slot.name))
          && (r.date == null ||
          (slot.date!=null && r.date.getTime <= slot.date.getTime))
          )
      case _ => true
    }

    if (!validRange) {
      logDebug(s"$slot rejected because it is not in range[${fromSgcRange.get}]")
      return None
    }

    // Reject name if it was considered earlier
    if (recentlySelectedFiles.contains(slot.name)) {
      logDebug(s"$slot rejected because it has been already considered")
      return None
    }

    logDebug(s"$slot rejected because it has been accepted" +
      s" with mod date ${slot.date}")

    return Option(slot)
  }

  /**
    * Internal method that check if a es.alvsanand.sgc.core.connector.SgcSlot
 *
    * @param slots
    * @return
    */
  private def slotsToRDD(slots: Array[A]): RDD[String] = {
    new SgcRDD[A, B](context.sparkContext, slots, sgcConnectorFactory,
      parameters, charset, maxRetries)
  }

  /** See [[https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.streaming.dst
    * ream.InputDStream]]
    */
  override def start(): Unit = {
  }

  /** See [[https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.streaming.dst
    * ream.InputDStream]]
    */
  override def stop(): Unit = {
  }

  /** See [[https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.streaming.dst
    * ream.InputDStream]]
    */
  private[streaming] override def name: String = s"Connector stream [$id]"

  /** See [[https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.streaming.dst
    * ream.InputDStream]]
    */
  private[streaming] override def clearMetadata(time: Time) {
    super.clearMetadata(time)

    batchTimeToSelectedFiles.synchronized {
      val oldFiles = batchTimeToSelectedFiles.filter(_._1 < (time - rememberDuration))
      batchTimeToSelectedFiles --= oldFiles.keys

      logDebug("Cleared slots are:\n" + oldFiles.map(p => (p._1, p._2)).mkString("\n"))
    }
  }

  @throws(classOf[IOException])
  private def readObject(ois: ObjectInputStream): Unit = Utils.tryOrIOException {
    logDebug(this.getClass().getSimpleName + ".readObject used")

    ois.defaultReadObject()
    generatedRDDs = new HashMap[Time, RDD[String]]()
    batchTimeToSelectedFiles = new mutable.HashMap[Time, Array[A]]
    recentlySelectedFiles = new mutable.HashSet[String]()
  }

  /**
    * This class represents the data saved when Spark performs a checkpoint.
    */
  private[streaming]
  class SgcInputDStreamCheckpointData extends DStreamCheckpointData(this) {

    /** See [[https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.streaming.d
      * stream.DStreamCheckpointData]]
      */
    override def update(time: Time) {
      processedSlots.clear()
      batchTimeToSelectedFiles.synchronized{
        processedSlots ++= batchTimeToSelectedFiles
      }
    }

    /** See [[https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.streaming.d
      * stream.DStreamCheckpointData]]
      */
    override def cleanup(time: Time) {}

    /** See [[https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.streaming.d
      * stream.DStreamCheckpointData]]
      */
    override def restore() {
      processedSlots.toSeq.sortBy(_._1)(Time.ordering).foreach {
        case (t: Time, f: Array[A]) => {
          // Restore the metadata in both slots and generatedRDDs
          logDebug(s"Restoring slots for time $t +${f.mkString("[", ", ", "]")}")
          batchTimeToSelectedFiles.synchronized {
            batchTimeToSelectedFiles += ((t, f))
          }
          recentlySelectedFiles ++= f.map(_.name)
          generatedRDDs += ((t, slotsToRDD(f)))
        }
      }
    }

    private def processedSlots = data.asInstanceOf[mutable.HashMap[Time, Array[A]]]

    /** See [[https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.streaming.d
      * stream.DStreamCheckpointData]]
      */
      override def toString: String = {
      "[\n" + processedSlots.size + " slot sets\n" +
        processedSlots.map(p => (p._1, p._2.mkString(", "))).mkString("\n") + "\n]"
    }
  }

}

