package es.alvsanand.sgdc.core.util

import java.io.OutputStream

import com.wix.accord.Validator
import com.wix.accord.dsl._
import es.alvsanand.sgdc.core.downloader._

/**
  * Created by alvsanand on 8/12/16.
  */
object SgdcDownloaderFactoryHelper {

  def createFactory(files: Seq[SgdcSlot], listBadTries: Int = 0, downloadBadTries: Int = 0,
                    splitInside: Boolean = false): SgdcDownloaderFactory[SgdcSlot,
    SgdcDownloaderParameters] = {
    new SgdcDownloaderFactory[SgdcSlot, SgdcDownloaderParameters]() {
      private var _downloadBadTries = 0
      private var _listBadTries = 0
      private var splitIndex = 0

      override def get(parameters: SgdcDownloaderParameters):
      SgdcDownloader[SgdcSlot, SgdcDownloaderParameters] =
        new SgdcDownloader[SgdcSlot, SgdcDownloaderParameters](parameters) {
          override def getValidator(): Validator[SgdcDownloaderParameters] =
            validator[SgdcDownloaderParameters] { p => }

          override def list(): Seq[SgdcSlot] = {
            if (_listBadTries < listBadTries) {
              _listBadTries += 1
              throw new Exception(s"Waiting until _downloadBadTries[${_listBadTries}]==0")
            }
            else {
              if (splitInside && splitIndex < files.size) {
                splitIndex = splitIndex + 1

                files.slice(0, splitIndex)
              } else files
            }
          }

          override def download(slot: SgdcSlot, out: OutputStream): Unit = {
            if (_downloadBadTries < downloadBadTries) {
              _downloadBadTries += 1
              throw new Exception(s"Waiting until _downloadBadTries[${_downloadBadTries}]==0")
            }
            else {
              IOUtils.copy(getClass.getResourceAsStream(slot.name), out)
            }
          }
        }
    }
  }

  def createDateFactory(files: Seq[SgdcDateSlot], listBadTries: Int = 0, downloadBadTries: Int = 0,
                        splitInside: Boolean = false): SgdcDownloaderFactory[SgdcDateSlot,
    SgdcDownloaderParameters] = {
    new SgdcDownloaderFactory[SgdcDateSlot, SgdcDownloaderParameters]() {
      private var _downloadBadTries = 0
      private var _listBadTries = 0
      private var splitIndex = 0

      override def get(parameters: SgdcDownloaderParameters):
      SgdcDownloader[SgdcDateSlot, SgdcDownloaderParameters] =
        new SgdcDownloader[SgdcDateSlot, SgdcDownloaderParameters](parameters) {
          override def getValidator(): Validator[SgdcDownloaderParameters] =
            validator[SgdcDownloaderParameters] { p => }

          override def list(): Seq[SgdcDateSlot] = {
            if (_listBadTries < listBadTries) {
              _listBadTries += 1
              throw new Exception(s"Waiting until _downloadBadTries[${_listBadTries}]==0")
            }
            else {
              if (splitInside && splitIndex < files.size) {
                splitIndex = splitIndex + 1

                files.slice(0, splitIndex)
              } else files
            }
          }

          override def download(slot: SgdcDateSlot, out: OutputStream): Unit = {
            if (_downloadBadTries < downloadBadTries) {
              _downloadBadTries += 1
              throw new Exception(s"Waiting until _downloadBadTries[${_downloadBadTries}]==0")
            }
            else {
              IOUtils.copy(getClass.getResourceAsStream(slot.name), out)
            }
          }
        }
    }
  }
}
