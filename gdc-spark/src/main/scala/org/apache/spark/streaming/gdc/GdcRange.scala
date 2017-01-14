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

import java.util.Date

/**
  * Trait that represents a range of slots. It is used by
  * org.apache.spark.streaming.gdc.GdcInputDStream in the beginning to filter the firsts
  * slots.
  */
sealed trait GdcRange

/**
  * The object has several utilities to create several types of ranges.
  */
object GdcRange {
  /**
    * Create a org.apache.spark.streaming.gdc.GdcRange.GdcSlotsRange.
    * @param slots The previously processed slot names.
    * @return The GdcRange
    */
  def apply(slots: Seq[String]): GdcRange = {
    new GdcSlotsRange(slots)
  }

  /**
    * Create a org.apache.spark.streaming.gdc.GdcRange.GdcDateRange.
    *
    * @param date The beginning date to look for slots.
    * @return The GdcRange
    */
  def apply(date: Date): GdcRange = {
    new GdcDateRange(date)
  }

  /**
    * Create a org.apache.spark.streaming.gdc.GdcRange.GdcDateSlotsRange.
    * @param date The beginning date to look for slots.
    * @param slots The previously processed slot names.
    * @return The GdcRange
    */
  def apply(date: Date, slots: Seq[String]): GdcRange = {
    new GdcDateSlotsRange(date, slots)
  }

  /**
    * An empty org.apache.spark.streaming.gdc.GdcRange implementation.
    */
  sealed case class GdcEmptyRange() extends GdcRange
  val Empty = GdcEmptyRange()

  /**
    * A org.apache.spark.streaming.gdc.GdcRange implementation that consisting of a beginning
    * date and previously processed slots. This is how
    * org.apache.spark.streaming.gdc.GdcInputDStream will use of it:
    *
    *  - Proccessed all the slots which are equal or newer that a beginning date.
    *
    *  - And slots which are not in the sequence of slots [akka previously processed slots].
    * @param slots The previously processed slot names.
    */
  case class GdcSlotsRange(val slots: Seq[String]) extends GdcRange

  /**
    * A org.apache.spark.streaming.gdc.GdcRange implementation that consisting of a beginning
    * date. This is how
    * org.apache.spark.streaming.gdc.GdcInputDStream will use of it:
    *
    *  - Proccessed all the slots which are equal or newer that a beginning date.
    * @param date The beginning date to look for slots.
    */
  case class GdcDateRange(val date: Date) extends GdcRange

  /**
    * A org.apache.spark.streaming.gdc.GdcRange implementation that consisting of a beginning
    * date and previously processed slots. This is how
    * org.apache.spark.streaming.gdc.GdcInputDStream will use of it:
    *  - Proccessed all the slots which are equal or newer that a beginning date.
    *  - And slots which are not in the sequence of slots [akka previously processed slots].
    * @param date The beginning date to look for slots.
    * @param slots The previously processed slot names.
    */
  case class GdcDateSlotsRange(val date: Date, val slots: Seq[String]) extends
    GdcRange
}
