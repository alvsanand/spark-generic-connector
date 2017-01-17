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

import es.alvsanand.sgc.core.connector.SgcSlot
import org.apache.spark.Partition

/**
  * A implementation of org.apache.spark.Partition for SgcR containing the slot information.
 *
  * @param slot The slot of the partition.
  * @param index The index of the partition.
  * @tparam A The type of es.alvsanand.sgc.core.connector.SgcSlot
  */
class SgcRDDPartition[A <: SgcSlot](
                                     val slot: A,
                                     val index: Int
                                   ) extends Partition {

  /**
    * The equality method for reference types.  Default implementation delegates to `eq`.
    * @param  other    the object to compare against this object for equality.
    * @return `true` if the receiver object is equivalent to the argument; `false` otherwise.
    */
  override def equals(other: Any): Boolean = other match {
    case that: SgcRDDPartition[A] =>
      (that canEqual this) &&
        slot == that.slot &&
        index == that.index
    case _ => false
  }

  /**
    * Return true if both objects are of the same Instance.
    * @param other The object to compare
    * @return True if both objects are of the same Instance.
    */
  private def canEqual(other: Any): Boolean = other.isInstanceOf[SgcRDDPartition[A]]


  /**
    * The hashCode method for reference types.
    * @return the hash code value for this object.
    */
  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), slot, index)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  /**
    * Creates a String representation of this object.  The default
    *  representation is platform dependent.  On the java platform it
    *  is the concatenation of the class name, "@", and the object's
    *  hashcode in hexadecimal.
    * @return a String representation of the object.
    */
  override def toString: String = s"SgcRDDPartition($slot, $index)"
}
