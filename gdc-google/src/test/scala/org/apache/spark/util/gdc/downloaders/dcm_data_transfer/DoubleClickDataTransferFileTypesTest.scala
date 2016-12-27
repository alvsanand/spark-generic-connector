package org.apache.spark.util.gdc.downloaders.dcm_data_transfer

import java.text.SimpleDateFormat

import org.scalatest._

/**
  * Created by alvsanand on 10/12/16.
  */
class DoubleClickDataTransferFileTypesTest extends FlatSpec with Matchers with OptionValues with Inside with Inspectors with BeforeAndAfterAll {

  it should "work getDoubleClickDataTransferFile" in {
    val sd: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    org.apache.spark.util.gdc.downloaders.dcm_data_transfer.DoubleClickDataTransferFileTypes.getDoubleClickDataTransferFile("dcm_account411205_activity_20160927_20160928_050546_293409263.csv.gz") should be(Option(new DoubleClickDataTransferFile("dcm_account411205_activity_20160927_20160928_050546_293409263.csv.gz", Option(sd.parse("2016-09-27 00:00:00:000")), Option(org.apache.spark.util.gdc.downloaders.dcm_data_transfer.DoubleClickDataTransferFileTypes.ACTIVITY))))
    org.apache.spark.util.gdc.downloaders.dcm_data_transfer.DoubleClickDataTransferFileTypes.getDoubleClickDataTransferFile("dcm_account411205_click_2016092602_20160926_133141_292763957.csv.gz") should be(Option(new DoubleClickDataTransferFile("dcm_account411205_click_2016092602_20160926_133141_292763957.csv.gz", Option(sd.parse("2016-09-26 02:00:00:000")), Option(org.apache.spark.util.gdc.downloaders.dcm_data_transfer.DoubleClickDataTransferFileTypes.CLICK))))
    org.apache.spark.util.gdc.downloaders.dcm_data_transfer.DoubleClickDataTransferFileTypes.getDoubleClickDataTransferFile("dcm_account411205_impression_2016092603_20160926_131805_292768332.csv.gz") should be(Option(new DoubleClickDataTransferFile("dcm_account411205_impression_2016092603_20160926_131805_292768332.csv.gz", Option(sd.parse("2016-09-26 03:00:00:000")), Option(org.apache.spark.util.gdc.downloaders.dcm_data_transfer.DoubleClickDataTransferFileTypes.IMPRESSION))))
    org.apache.spark.util.gdc.downloaders.dcm_data_transfer.DoubleClickDataTransferFileTypes.getDoubleClickDataTransferFile("FOOO") should be(None)
    org.apache.spark.util.gdc.downloaders.dcm_data_transfer.DoubleClickDataTransferFileTypes.getDoubleClickDataTransferFile("dcm_account411205_FOO_2016092603_20160926_131805_292768332.csv.gz") should be(None)
  }

  it should "work getType" in {
    val sd: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    org.apache.spark.util.gdc.downloaders.dcm_data_transfer.DoubleClickDataTransferFileTypes.getType("ACTIVITY") should be(Option(org.apache.spark.util.gdc.downloaders.dcm_data_transfer.DoubleClickDataTransferFileTypes.ACTIVITY))
    org.apache.spark.util.gdc.downloaders.dcm_data_transfer.DoubleClickDataTransferFileTypes.getType("CLICK") should be(Option(org.apache.spark.util.gdc.downloaders.dcm_data_transfer.DoubleClickDataTransferFileTypes.CLICK))
    org.apache.spark.util.gdc.downloaders.dcm_data_transfer.DoubleClickDataTransferFileTypes.getType("IMPRESSION") should be(Option(org.apache.spark.util.gdc.downloaders.dcm_data_transfer.DoubleClickDataTransferFileTypes.IMPRESSION))
  }
}
