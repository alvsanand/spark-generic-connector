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

sealed trait GdcRange

object GdcRange {
  def apply(date: Date, files: String*): GdcRange = {
    new DateFilesGdcRange(date, files)
  }

  def apply(date: Date): GdcRange = {
    new DateGdcRange(date)
  }

  case class DateFilesGdcRange(val date: Date, val files: Seq[String]) extends
    GdcRange

  case class DateGdcRange(val date: Date) extends GdcRange
}
