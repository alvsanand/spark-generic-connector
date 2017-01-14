package es.alvsanand.gdc.core.util

import java.io.OutputStream

import com.wix.accord.Validator
import com.wix.accord.dsl._
import es.alvsanand.gdc.core.downloader._

/**
  * Created by alvsanand on 8/12/16.
  */
object GdcDownloaderFactoryHelper {

  def createFactory(files: Seq[GdcSlot], listBadTries: Int = 0, downloadBadTries: Int = 0,
                    splitInside: Boolean = false): GdcDownloaderFactory[GdcSlot,
    GdcDownloaderParameters] = {
    new GdcDownloaderFactory[GdcSlot, GdcDownloaderParameters]() {
      private var _downloadBadTries = 0
      private var _listBadTries = 0
      private var splitIndex = 0

      override def get(parameters: GdcDownloaderParameters):
      GdcDownloader[GdcSlot, GdcDownloaderParameters] =
        new GdcDownloader[GdcSlot, GdcDownloaderParameters](parameters) {
          override def getValidator(): Validator[GdcDownloaderParameters] =
            validator[GdcDownloaderParameters] { p => }

          override def list(): Seq[GdcSlot] = {
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

          override def download(file: GdcSlot, out: OutputStream): Unit = {
            if (_downloadBadTries < downloadBadTries) {
              _downloadBadTries += 1
              throw new Exception(s"Waiting until _downloadBadTries[${_downloadBadTries}]==0")
            }
            else {
              IOUtils.copy(getClass.getResourceAsStream(file.name), out)
            }
          }
        }
    }
  }

  def createDateFactory(files: Seq[GdcDateSlot], listBadTries: Int = 0, downloadBadTries: Int = 0,
                        splitInside: Boolean = false): GdcDownloaderFactory[GdcDateSlot,
    GdcDownloaderParameters] = {
    new GdcDownloaderFactory[GdcDateSlot, GdcDownloaderParameters]() {
      private var _downloadBadTries = 0
      private var _listBadTries = 0
      private var splitIndex = 0

      override def get(parameters: GdcDownloaderParameters):
      GdcDownloader[GdcDateSlot, GdcDownloaderParameters] =
        new GdcDownloader[GdcDateSlot, GdcDownloaderParameters](parameters) {
          override def getValidator(): Validator[GdcDownloaderParameters] =
            validator[GdcDownloaderParameters] { p => }

          override def list(): Seq[GdcDateSlot] = {
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

          override def download(file: GdcDateSlot, out: OutputStream): Unit = {
            if (_downloadBadTries < downloadBadTries) {
              _downloadBadTries += 1
              throw new Exception(s"Waiting until _downloadBadTries[${_downloadBadTries}]==0")
            }
            else {
              IOUtils.copy(getClass.getResourceAsStream(file.name), out)
            }
          }
        }
    }
  }
}
