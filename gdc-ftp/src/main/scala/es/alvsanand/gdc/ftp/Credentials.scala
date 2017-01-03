package es.alvsanand.gdc.ftp

/**
  * Created by alvsanand on 3/01/17.
  */
trait Credentials

trait HasUserPassword {
  def user: String
  def password: String
}

trait HasPrivateKey {
  def keystore: String
  def keystorePassword: String
}

case class PrivateKeyCredentials(keystore: String, keystorePassword: String)
  extends Credentials with HasPrivateKey

case class UserPasswordCredentials(user: String, password: String)
  extends Credentials with HasUserPassword

case class UserPasswordAndPrivateKeyCredentials(user: String, password: String,
                                                keystore: String, keystorePassword: String)
  extends Credentials with HasUserPassword with HasPrivateKey
