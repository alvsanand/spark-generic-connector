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

package es.alvsanand.gdc.google.cloud_storage

import org.scalatest._

class CloudStorageGdcDownloaderFactoryTest extends FlatSpec with Matchers with OptionValues
  with Inside with  Inspectors with BeforeAndAfterAll {

  it should "fail with empty parameters" in {
    a[IllegalArgumentException] shouldBe thrownBy(CloudStorageGdcDownloaderFactory
      .get(Map[String, String]()))
    a[IllegalArgumentException] shouldBe thrownBy(CloudStorageGdcDownloaderFactory
      .get(Map[String, String]("credentialsPath" -> "")))
    a[IllegalArgumentException] shouldBe thrownBy(CloudStorageGdcDownloaderFactory
      .get(Map[String, String]("bucket" -> "")))
    a[IllegalArgumentException] shouldBe thrownBy(CloudStorageGdcDownloaderFactory
      .get(Map[String, String]("credentialsPath" -> "", "bucket" -> "")))
    a[IllegalArgumentException] shouldBe thrownBy(CloudStorageGdcDownloaderFactory
      .get(Map[String, String]("credentialsPath" -> "FOO", "bucket" -> "")))
    a[IllegalArgumentException] shouldBe thrownBy(CloudStorageGdcDownloaderFactory
      .get(Map[String, String]("credentialsPath" -> "", "bucket" -> "FOO")))
  }

  it should "work with obligatory parameters" in {
    CloudStorageGdcDownloaderFactory
      .get(Map[String, String]("credentialsPath" -> "FOO", "bucket" -> "FOO")) should not be (null)
  }
}
