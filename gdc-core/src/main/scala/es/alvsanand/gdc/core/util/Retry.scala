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

package es.alvsanand.gdc.core.util

import scala.util.{Failure, Success, Try}

/**
  * Created by alvsanand on 27/06/16.
  */
object Retry extends Logging {

  @annotation.tailrec
  def apply[T](n: Int, sleepTime: Int = 100)(fn: => T): Try[T] = {
    Try {
      fn
    } match {
      case x: Success[T] => x
      case Failure(e) if n > 1 => {
        logError(s"Received unexpected error. Retrying[sleepTime: $sleepTime], retries: $n", e)

        Thread.sleep(sleepTime)

        apply(n - 1, sleepTime)(fn)
      }
      case Failure(e) => Failure(e)
    }
  }
}
