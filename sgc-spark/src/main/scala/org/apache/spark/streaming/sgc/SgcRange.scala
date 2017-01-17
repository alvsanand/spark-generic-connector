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

import java.util.Date

/**
  * Trait that represents a range of slots. It is used by
  * org.apache.spark.streaming.sgc.SgcInputDStream in the beginning to filter the firsts
  * slots.
  */
sealed trait SgcRange

/**
  * The object has several utilities to create several types of ranges.
  */
object SgcRange {
  /**
    * Create a org.apache.spark.streaming.sgc.SgcRange.SgcSlotsRange.
    * @param slots The previously processed slot names.
    * @return The SgcRange
    */
  def apply(slots: Seq[String]): SgcRange = {
    new SgcSlotsRange(slots)
  }

  /**
    * Create a org.apache.spark.streaming.sgc.SgcRange.SgcDateRange.
    *
    * @param date The beginning date to look for slots.
    * @return The SgcRange
    */
  def apply(date: Date): SgcRange = {
    new SgcDateRange(date)
  }

  /**
    * Create a org.apache.spark.streaming.sgc.SgcRange.SgcDateSlotsRange.
    * @param date The beginning date to look for slots.
    * @param slots The previously processed slot names.
    * @return The SgcRange
    */
  def apply(date: Date, slots: Seq[String]): SgcRange = {
    new SgcDateSlotsRange(date, slots)
  }

  /**
    * An empty org.apache.spark.streaming.sgc.SgcRange implementation.
    */
  sealed case class SgcEmptyRange() extends SgcRange
  val Empty = SgcEmptyRange()

  /**
    * A org.apache.spark.streaming.sgc.SgcRange implementation that consisting of a beginning
    * date and previously processed slots. This is how
    * org.apache.spark.streaming.sgc.SgcInputDStream will use of it:
    *
    *  - Proccessed all the slots which are equal or newer that a beginning date.
    *
    *  - And slots which are not in the sequence of slots [akka previously processed slots].
    * @param slots The previously processed slot names.
    */
  case class SgcSlotsRange(val slots: Seq[String]) extends SgcRange

  /**
    * A org.apache.spark.streaming.sgc.SgcRange implementation that consisting of a beginning
    * date. This is how
    * org.apache.spark.streaming.sgc.SgcInputDStream will use of it:
    *
    *  - Proccessed all the slots which are equal or newer that a beginning date.
    * @param date The beginning date to look for slots.
    */
  case class SgcDateRange(val date: Date) extends SgcRange

  /**
    * A org.apache.spark.streaming.sgc.SgcRange implementation that consisting of a beginning
    * date and previously processed slots. This is how
    * org.apache.spark.streaming.sgc.SgcInputDStream will use of it:
    *  - Proccessed all the slots which are equal or newer that a beginning date.
    *  - And slots which are not in the sequence of slots [akka previously processed slots].
    * @param date The beginning date to look for slots.
    * @param slots The previously processed slot names.
    */
  case class SgcDateSlotsRange(val date: Date, val slots: Seq[String]) extends
    SgcRange
}
