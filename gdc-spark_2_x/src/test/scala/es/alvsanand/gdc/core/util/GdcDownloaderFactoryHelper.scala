package es.alvsanand.gdc.core.util

import java.io.OutputStream

import es.alvsanand.gdc.core.downloader.{GdcDownloader, GdcDownloaderFactory, GdcFile}

/**
  * Created by alvsanand on 8/12/16.
  */
object GdcDownloaderFactoryHelper {

  def createDownloaderFactory(files: Seq[GdcFile], listBadTries: Int = 0, downloadBadTries: Int = 0,
                              splitInside: Boolean = false): GdcDownloaderFactory[GdcFile] = {
    new GdcDownloaderFactory[GdcFile]() {
      private var _downloadBadTries = downloadBadTries
      private var _listBadTries = listBadTries
      private var splitIndex = 0

      override def get(gdcDownloaderParams: Map[String, String]): GdcDownloader[GdcFile] = new
          GdcDownloader[GdcFile]() {
        override def list(): Seq[GdcFile] = {
          if (_listBadTries > 0) {
            _listBadTries = _listBadTries - 1
            throw new Exception(s"Waiting until _downloadBadTries[${_listBadTries}]==0")
          }
          else {
            if (splitInside && splitIndex < files.size) {
              splitIndex = splitIndex + 1

              files.slice(0, splitIndex)
            } else files
          }
        }

        override def download(file: GdcFile, out: OutputStream): Unit = {
          if (_downloadBadTries > 0) {
            _downloadBadTries = _downloadBadTries - 1
            throw new Exception(s"Waiting until _downloadBadTries[${_downloadBadTries}]==0")
          }
          else {
            IOUtils.copy(getClass.getResourceAsStream(file.file), out)
          }
        }
      }
    }
  }

}
