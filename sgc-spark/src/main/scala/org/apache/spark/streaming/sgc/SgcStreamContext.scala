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

package org.apache.spark.streaming.sgc

import es.alvsanand.sgc.core.connector.{SgcConnectorFactory, SgcConnectorParameters, SgcDateSlot}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream

import scala.reflect.ClassTag

/**
  * SgcStreamContext creates a InputDStreams which is able to read slots using a
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
  * @param ssc The StreamingContext
  */
case class SgcStreamContext(@transient ssc: StreamingContext) {

  /**
    * * Create a org.apache.spark.streaming.sgc.SgcInputDStream from a SparkContext using a
    * es.alvsanand.sgc.core.connector.SgcConnectorFactory and some parameters.
 *
    * @param sgcConnectorFactory The SgcConnectorFactory used to create the
    *                             es.alvsanand.sgc.core.connector.SgcConnector.
    * @param parameters The parameters of the es.alvsanand.sgc.core.connector.SgcConnector.
    * @param fromSgcRange The range from where the SgcInputDStream must begin.
    * @param charset The java.nio.charset.Charset name of the slots that are going to be
    *                fetched.
    * @param maxRetries  The maximum number times that an operation of a
    *                   es.alvsanand.sgc.core.connector.SgcConnector is going to be repeated
    *                   in case of failure.
    * @tparam A The type of es.alvsanand.sgc.core.connector.SgcDateSlot
    * @tparam B The type of es.alvsanand.sgc.core.connector.SgcConnectorParameters
    * @return A SgcInputDStream
    */
  def createSgcInputDStream[A <: SgcDateSlot: ClassTag, B <: SgcConnectorParameters: ClassTag]
                                                  (sgcConnectorFactory: SgcConnectorFactory[A, B],
                                                   parameters: B,
                                                   fromSgcRange: Option[SgcRange] = None,
                                                   charset: String = "UTF-8",
                                                   maxRetries: Int = 3): InputDStream[String] =
    new SgcInputDStream[A, B](ssc, sgcConnectorFactory, parameters,
      fromSgcRange, charset, maxRetries)
}
