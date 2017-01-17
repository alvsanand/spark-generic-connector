package es.alvsanand.sgc.core.util

import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._
import org.scalatest.concurrent.TimeLimitedTests
import org.scalatest.time.SpanSugar._

/**
  * Created by alvsanand on 10/12/16.
  */
class SparkTest extends FlatSpec with Matchers with OptionValues with Inside with Inspectors with
  BeforeAndAfterAll
  with TimeLimitedTests {
  val dt: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  val timeLimit = 120 seconds

  protected def sc() = SparkTest.sc()
}

object SparkTest {

  val conf = new SparkConf().setAppName("SparkTest").setMaster("local[*]")
  conf.set("spark.ui.enabled", "false")

  private val _sc = new SparkContext(conf)

  protected def sc() = _sc
}
