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

package es.alvsanand.sgc.core.util

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import org.scalatest._

import scala.collection.JavaConversions._
import scala.util.{Failure, Success}

class RetryTest extends FlatSpec with Matchers with OptionValues
  with Inside with Inspectors with BeforeAndAfterAll {

  it should "Check enough retries [maxFails < retries]" in {
    var fails = 0
    val maxFails = 2

    def method(): Boolean = {
      if (fails < maxFails) {
        fails += 1
        throw new Exception()
      }
      true
    }

    Retry(3){method()}.isSuccess should be(true)
  }

  it should "Check enough retries [maxFails == retries]" in {
    var fails = 0
    val maxFails = 3

    def method(): Boolean = {
      if (fails < maxFails) {
        fails += 1
        throw new Exception()
      }
      true
    }

    Retry(3)({method()}).isSuccess should be(true)
  }

  it should "Check not enough retries" in {
    var fails = 0
    val maxFails = 5

    def method(): Boolean = {
      if (fails < maxFails) {
        fails += 1
        throw new Exception()
      }
      true
    }

    Retry(3)({method()}).isFailure should be(true)
  }
}
