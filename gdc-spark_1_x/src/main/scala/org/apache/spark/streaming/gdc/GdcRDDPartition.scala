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

import es.alvsanand.gdc.core.downloader.GdcFile
import org.apache.spark.Partition

class GdcRDDPartition[A <: GdcFile](
                                     val gdcFile: A,
                                     val index: Int
                                   ) extends Partition {

  override def equals(other: Any): Boolean = other match {
    case that: GdcRDDPartition[A] =>
      (that canEqual this) &&
        gdcFile == that.gdcFile &&
        index == that.index
    case _ => false
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[GdcRDDPartition[A]]

  override def hashCode(): Int = {
    val state = Seq(super.hashCode(), gdcFile, index)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"DownloadRDDPartition($GdcFile, $index)"
}
