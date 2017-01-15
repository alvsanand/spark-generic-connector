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

package org.apache.spark.streaming

import org.apache.spark.SparkContext

/**
  * This Package object ease to developers the creation of org.apache.spark.streaming.sgdc.SgdcRDD
  * and org.apache.spark.streaming.sgdc.SgdcInputDStream.
  */
package object sgdc {
  /**
    * Creates a org.apache.spark.streaming.sgdc.SgdcContext and set as a function of the
    * org.apache.spark.SparkContext
    * @param sc The SparkContext
    * @return A SgdcContext
    */
  implicit def toSparkContextFunctions(sc: SparkContext): SgdcContext =
    SgdcContext(sc)

  /**
    * Creates a org.apache.spark.streaming.sgdc.SgdcStreamContext and set as a function of the
    * org.apache.spark.streaming.StreamingContext
    * @param ssc the StreamingContext
    * @return A SgdcStreamContext
    */
  implicit def toSparkContextFunctions(ssc: StreamingContext): SgdcStreamContext =
    SgdcStreamContext(ssc)
}
