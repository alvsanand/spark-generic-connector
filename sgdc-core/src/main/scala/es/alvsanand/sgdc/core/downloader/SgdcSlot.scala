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

package es.alvsanand.sgdc.core.downloader

import java.util.Date

/**
  * This trait represents a slot of a es.alvsanand.sgdc.core.downloader.SgdcDownloader. A SgdcSlot
  * is the smallest part than a es.alvsanand.sgdc.core.downloader.SgdcDownloader can download in a single time.
  */
trait SgdcSlot {
  /**
    * The name of the Slot.
    */
  val name: String

  /**
    * Compares with another SgdcSlot
    * @param that The other SgdcSlot
    * @return -1 if it the current is before, 0 if both files are in the same position or 1 if it is
    *         after the other name.
    */
  def compare(that: SgdcSlot): Int = {
    if (that==null){
      return -1
    }

    if (this.name != that.name) return this.name.compare(that.name)

    return 0
  }
}

/**
  * This trait represents a slot of a es.alvsanand.sgdc.core.downloader.SgdcDownloader which has
  * a date associated to it.
  *
  * Note: this trait must be used instead of
  * es.alvsanand.sgdc.core.downloader.SgdcSlot in a Streaming scenario.
  */
trait SgdcDateSlot extends SgdcSlot{
  /**
    * The creation date of the name if it can be obtained.
    */
  val date: Date

  /**
    * Compares with another SgdcSlot
    * @param that The other SgdcSlot
    * @return -1 if it the current is before, 0 if both files are in the same position or 1 if it is
    *         after the other name.
    */
  override def compare(that: SgdcSlot): Int = {
    if (that==null){
      return -1
    }

    if (that.isInstanceOf[SgdcDateSlot]) {
      val thatDate = that.asInstanceOf[SgdcDateSlot]

      if (this.date == null && thatDate.date != null) {
        return 1
      }
      if (this.date != null && thatDate.date == null) {
        return -1
      }
      if (this.date.getTime != thatDate.date.getTime) {
        return this.date.getTime.compare(thatDate.date.getTime)
      }
    }

    if (this.name != that.name) return this.name.compare(that.name)

    return 0
  }
}

/**
  * Creates basic implementations of a SgdcSlot.
  */
object SgdcSlot {
  /**
    * Creates a basic SgdcSlot
    * @param slot The name of the slot
    * @return A basic SgdcSlot instance
    */
  def apply(slot: String): SgdcSlot = {
    new SgdcSlotImpl(slot)
  }

  /**
    * Creates a basic SgdcSlot
    * @param slot The name of the slot
    * @param date The creation date of the name if it can be obtained.
    * @return A basic SgdcSlot instance
    */
  def apply(slot: String, date: Date): SgdcDateSlot = {
    new SgdcDateSlotImpl(slot, date)
  }

  /**
    * Private basic implementation of the es.alvsanand.sgdc.core.downloader.SgdcSlot trait.
    *
    * @param name The name of the name
    */
  private case class SgdcSlotImpl(name: String) extends SgdcSlot

  /**
    * Private basic implementation of the es.alvsanand.sgdc.core.downloader.SgdcDateSlot trait.
    *
    * @param name The name of the name
    * @param date The creation date of the name if it can be obtained.
    */
  private case class SgdcDateSlotImpl(name: String, date: Date) extends SgdcDateSlot
}
