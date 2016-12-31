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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import org.scalatest._
import scala.collection.JavaConversions._

class IOUtilsTest extends FlatSpec with Matchers with OptionValues
  with Inside with  Inspectors with BeforeAndAfterAll {

  it should "Check parseInputStream Normal File" in {
    val file = "/files/example.txt"

    val in = getClass.getResourceAsStream(file)
    val out = IOUtils.parseInputStream(in)

    out should not be null

    org.apache.commons.io.IOUtils.readLines(out)
      .toList should be(List("LINE 001", "LINE 002", "LINE 003", "LINE 004", "LINE 005"))
  }

  it should "Check parseInputStream GZNormal File" in {
    val file = "/files/example.txt.gz"

    val in = getClass.getResourceAsStream(file)
    val out = IOUtils.parseInputStream(in)

    out should not be null

    org.apache.commons.io.IOUtils.readLines(out)
      .toList should be(List("LINE 001", "LINE 002", "LINE 003", "LINE 004", "LINE 005"))
  }

  it should "Check copy" in {
    val s = "FOO"

    val in = new ByteArrayInputStream(s.getBytes)
    val out = new ByteArrayOutputStream()

    IOUtils.copy(in, out)

    new String(out.toByteArray) should be(s)
  }

  it should "Check createTempDirectory" in {
    val dir = IOUtils.createTempDirectory()

    dir should not be null
    dir.isDirectory() should be(true)

    dir.delete()
  }
}
