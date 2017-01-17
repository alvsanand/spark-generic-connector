package es.alvsanand.sgc.core.util

import java.io.OutputStream

import com.wix.accord.Validator
import com.wix.accord.dsl._
import es.alvsanand.sgc.core.connector._

/**
  * Created by alvsanand on 8/12/16.
  */
object SgcConnectorFactoryHelper {

  def createFactory(files: Seq[SgcSlot], listBadTries: Int = 0, fetchBadTries: Int = 0,
                    splitInside: Boolean = false): SgcConnectorFactory[SgcSlot,
    SgcConnectorParameters] = {
    new SgcConnectorFactory[SgcSlot, SgcConnectorParameters]() {
      private var _fetchBadTries = 0
      private var _listBadTries = 0
      private var splitIndex = 0

      override def get(parameters: SgcConnectorParameters):
      SgcConnector[SgcSlot, SgcConnectorParameters] =
        new SgcConnector[SgcSlot, SgcConnectorParameters](parameters) {
          override def getValidator(): Validator[SgcConnectorParameters] =
            validator[SgcConnectorParameters] { p => }

          override def list(): Seq[SgcSlot] = {
            if (_listBadTries < listBadTries) {
              _listBadTries += 1
              throw new Exception(s"Waiting until _fetchBadTries[${_listBadTries}]==0")
            }
            else {
              if (splitInside && splitIndex < files.size) {
                splitIndex = splitIndex + 1

                files.slice(0, splitIndex)
              } else files
            }
          }

          override def fetch(slot: SgcSlot, out: OutputStream): Unit = {
            if (_fetchBadTries < fetchBadTries) {
              _fetchBadTries += 1
              throw new Exception(s"Waiting until _fetchBadTries[${_fetchBadTries}]==0")
            }
            else {
              IOUtils.copy(getClass.getResourceAsStream(slot.name), out)
            }
          }
        }
    }
  }

  def createDateFactory(files: Seq[SgcDateSlot], listBadTries: Int = 0, fetchBadTries: Int = 0,
                        splitInside: Boolean = false): SgcConnectorFactory[SgcDateSlot,
    SgcConnectorParameters] = {
    new SgcConnectorFactory[SgcDateSlot, SgcConnectorParameters]() {
      private var _fetchBadTries = 0
      private var _listBadTries = 0
      private var splitIndex = 0

      override def get(parameters: SgcConnectorParameters):
      SgcConnector[SgcDateSlot, SgcConnectorParameters] =
        new SgcConnector[SgcDateSlot, SgcConnectorParameters](parameters) {
          override def getValidator(): Validator[SgcConnectorParameters] =
            validator[SgcConnectorParameters] { p => }

          override def list(): Seq[SgcDateSlot] = {
            if (_listBadTries < listBadTries) {
              _listBadTries += 1
              throw new Exception(s"Waiting until _fetchBadTries[${_listBadTries}]==0")
            }
            else {
              if (splitInside && splitIndex < files.size) {
                splitIndex = splitIndex + 1

                files.slice(0, splitIndex)
              } else files
            }
          }

          override def fetch(slot: SgcDateSlot, out: OutputStream): Unit = {
            if (_fetchBadTries < fetchBadTries) {
              _fetchBadTries += 1
              throw new Exception(s"Waiting until _fetchBadTries[${_fetchBadTries}]==0")
            }
            else {
              IOUtils.copy(getClass.getResourceAsStream(slot.name), out)
            }
          }
        }
    }
  }
}
