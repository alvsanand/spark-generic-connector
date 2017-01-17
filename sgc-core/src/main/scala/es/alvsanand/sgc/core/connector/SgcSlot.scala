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

package es.alvsanand.sgc.core.connector

import java.util.Date

/**
  * This trait represents a slot of a es.alvsanand.sgc.core.connector.SgcConnector. A SgcSlot
  * is the smallest part than a es.alvsanand.sgc.core.connector.SgcConnector can fetch in a single time.
  */
trait SgcSlot {
  /**
    * The name of the Slot.
    */
  val name: String

  /**
    * Compares with another SgcSlot
    * @param that The other SgcSlot
    * @return -1 if it the current is before, 0 if both files are in the same position or 1 if it is
    *         after the other name.
    */
  def compare(that: SgcSlot): Int = {
    if (that==null){
      return -1
    }

    if (this.name != that.name) return this.name.compare(that.name)

    return 0
  }
}

/**
  * This trait represents a slot of a es.alvsanand.sgc.core.connector.SgcConnector which has
  * a date associated to it.
  *
  * Note: this trait must be used instead of
  * es.alvsanand.sgc.core.connector.SgcSlot in a Streaming scenario.
  */
trait SgcDateSlot extends SgcSlot{
  /**
    * The creation date of the name if it can be obtained.
    */
  val date: Date

  /**
    * Compares with another SgcSlot
    * @param that The other SgcSlot
    * @return -1 if it the current is before, 0 if both files are in the same position or 1 if it is
    *         after the other name.
    */
  override def compare(that: SgcSlot): Int = {
    if (that==null){
      return -1
    }

    if (that.isInstanceOf[SgcDateSlot]) {
      val thatDate = that.asInstanceOf[SgcDateSlot]

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
  * Creates basic implementations of a SgcSlot.
  */
object SgcSlot {
  /**
    * Creates a basic SgcSlot
    * @param slot The name of the slot
    * @return A basic SgcSlot instance
    */
  def apply(slot: String): SgcSlot = {
    new SgcSlotImpl(slot)
  }

  /**
    * Creates a basic SgcSlot
    * @param slot The name of the slot
    * @param date The creation date of the name if it can be obtained.
    * @return A basic SgcSlot instance
    */
  def apply(slot: String, date: Date): SgcDateSlot = {
    new SgcDateSlotImpl(slot, date)
  }

  /**
    * Private basic implementation of the es.alvsanand.sgc.core.connector.SgcSlot trait.
    *
    * @param name The name of the name
    */
  private case class SgcSlotImpl(name: String) extends SgcSlot

  /**
    * Private basic implementation of the es.alvsanand.sgc.core.connector.SgcDateSlot trait.
    *
    * @param name The name of the name
    * @param date The creation date of the name if it can be obtained.
    */
  private case class SgcDateSlotImpl(name: String, date: Date) extends SgcDateSlot
}
