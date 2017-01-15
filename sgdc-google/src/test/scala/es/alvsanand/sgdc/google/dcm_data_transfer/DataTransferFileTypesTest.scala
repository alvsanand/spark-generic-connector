package es.alvsanand.sgdc.google.dcm_data_transfer

import java.text.SimpleDateFormat

import org.scalatest._

/**
  * Created by alvsanand on 10/12/16.
  */
class DataTransferFileTypesTest extends FlatSpec with Matchers with OptionValues with Inside with
  Inspectors with BeforeAndAfterAll {

  val sd: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  it should "work getDoubleClickDataTransferFile" in {

    val fileA = "dcm_account411205_activity_20160927_20160928_050546_293409263.csv.gz"
    val dateA = sd.parse("2016-09-27 00:00:00:000")
    val typeA = Option(DataTransferFileTypes.ACTIVITY)
    val dtfA = Option(new DataTransferSlot(fileA, dateA, typeA))
    DataTransferFileTypes.getDataTransferFile(fileA) should be(dtfA)

    val fileB = "dcm_account411205_click_2016092602_20160926_133141_292763957.csv.gz"
    val dateB = sd.parse("2016-09-26 02:00:00:000")
    val typeB = Option(DataTransferFileTypes.CLICK)
    val dtfB = new DataTransferSlot(fileB, dateB, typeB)
    DataTransferFileTypes.getDataTransferFile(fileB) should be(Option(dtfB))

    val fileC = "dcm_account411205_impression_2016092603_20160926_131805_292768332.csv.gz"
    val dateC = sd.parse("2016-09-26 03:00:00:000")
    val typeC = Option(DataTransferFileTypes.IMPRESSION)
    val dtfC = new DataTransferSlot(fileC, dateC, typeC)
    DataTransferFileTypes.getDataTransferFile(fileC) should be(Option(dtfC))

    val fileD = "FOOO"
    DataTransferFileTypes.getDataTransferFile(fileD) should be(None)

    val fileE = "dcm_account411205_FOO_2016092603_20160926_131805_292768332.csv.gz"
    DataTransferFileTypes.getDataTransferFile(fileE) should be(None)
  }

  it should "work getType" in {
    val typeA = Option(DataTransferFileTypes.ACTIVITY)
    DataTransferFileTypes.getType("ACTIVITY") should be(typeA)

    val typeB = Option(DataTransferFileTypes.CLICK)
    DataTransferFileTypes.getType("CLICK") should be(typeB)

    val typeC = Option(DataTransferFileTypes.IMPRESSION)
    DataTransferFileTypes.getType("IMPRESSION") should be(typeC)
  }
}
