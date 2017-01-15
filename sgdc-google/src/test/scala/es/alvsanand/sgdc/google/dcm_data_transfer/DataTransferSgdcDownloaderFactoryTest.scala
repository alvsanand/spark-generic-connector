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

package es.alvsanand.sgdc.google.dcm_data_transfer

import org.scalatest._

class DataTransferSgdcDownloaderFactoryTest extends FlatSpec with Matchers with OptionValues
  with Inside with  Inspectors with BeforeAndAfterAll {

  it should "fail with empty parameters" in {
    a[IllegalArgumentException] shouldBe thrownBy(DataTransferSgdcDownloaderFactory
      .get(DataTransferParameters(null, null)))
    a[IllegalArgumentException] shouldBe thrownBy(DataTransferSgdcDownloaderFactory
      .get(DataTransferParameters("FOO", null)))
    a[IllegalArgumentException] shouldBe thrownBy(DataTransferSgdcDownloaderFactory
      .get(DataTransferParameters(null, "FOO")))
    a[IllegalArgumentException] shouldBe thrownBy(DataTransferSgdcDownloaderFactory
      .get(DataTransferParameters("", "")))
    a[IllegalArgumentException] shouldBe thrownBy(DataTransferSgdcDownloaderFactory
      .get(DataTransferParameters("FOO", "")))
    a[IllegalArgumentException] shouldBe thrownBy(DataTransferSgdcDownloaderFactory
      .get(DataTransferParameters("", "FOO")))
  }

  it should "work with obligatory parameters" in {
    DataTransferSgdcDownloaderFactory.get(DataTransferParameters("FOO", "FOO")) should not be (null)
  }
}
