package es.alvsanand.gdc.ftp

/**
  * Created by alvsanand on 3/01/17.
  */
case class Credentials(user: String, password: Option[String] = None,
                       keystore: Option[String] = None, keystorePassword: Option[String] = None) {
  def hasKeystore(): Boolean = keystore.isDefined

  override def toString: String = s"UserPasswordCredentials($user, ***, $keystore, ***)"
}
