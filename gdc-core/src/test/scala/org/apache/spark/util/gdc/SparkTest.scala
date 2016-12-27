package org.apache.spark.util.gdc

import java.text.SimpleDateFormat

import org.apache.spark.SparkContext
import org.scalatest._
import org.scalatest.concurrent.TimeLimitedTests
import org.scalatest.time.SpanSugar._

/**
  * Created by alvsanand on 10/12/16.
  */
class SparkTest extends FlatSpec with Matchers with OptionValues with Inside with Inspectors with BeforeAndAfterAll with TimeLimitedTests {
  val dt: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  val timeLimit = 120 seconds

  protected def sc() = SparkTest.sc()
}

object SparkTest {
  private val _sc = new SparkContext("local", "test")

  protected def sc() = _sc
}
