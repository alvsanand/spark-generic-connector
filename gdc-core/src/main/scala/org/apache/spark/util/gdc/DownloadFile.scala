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

package org.apache.spark.util.gdc

import java.util.Date

/**
  * Created by alvsanand on 11/10/16.
  */
trait DownloadFile extends Ordered[DownloadFile] {
  val file: String
  val date: Option[Date]

  def compare(that: DownloadFile): Int = {
    if (this.date.isDefined && !that.date.isDefined) {
      return 1
    }
    if (!this.date.isDefined && that.date.isDefined) {
      return -1
    }
    if (this.date.get.getTime != that.date.get.getTime) return this.date.get.getTime.compare(that.date.get.getTime)

    if (this.file != that.file) return this.file.compare(that.file)

    return 0
  }
}

object DownloadFile {
  def apply(file: String): DownloadFile = {
    new DownloadFileImpl(file)
  }

  def apply(file: String, date: Option[Date]): DownloadFile = {
    new DownloadFileImpl(file, date)
  }

  def apply(file: String, date: Date): DownloadFile = {
    new DownloadFileImpl(file, Option(date))
  }

  private case class DownloadFileImpl(file: String, date: Option[Date] = None) extends DownloadFile

}
