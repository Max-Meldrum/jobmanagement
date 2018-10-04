package runtime.appmanager.yarn.client

import java.security.PrivilegedExceptionAction

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.Records


private[yarn] class Client(conf: YarnConfiguration) extends LazyLogging {
  private val client = YarnClient.createYarnClient()
  client.init(conf)

  // Default priority set for all launched AMs
  private final val PRIORITY = 1
  // Default value if user.name is not found
  private final val DEFAULT_USER = "cda"


  def launchUnmanagedAM(id: Option[String] = None): Option[ApplicationId] =
    try {
      client.start()
      val appContext = client.createApplication()
        .getApplicationSubmissionContext

      val appId = appContext.getApplicationId

      val priority = Records.newRecord(classOf[Priority])
      priority.setPriority(PRIORITY)
      appContext.setPriority(priority)

      val amContainer = Records.newRecord(classOf[ContainerLaunchContext])
      appContext.setUnmanagedAM(true)

      appContext.setKeepContainersAcrossApplicationAttempts(true)
      appContext.setApplicationName(id.getOrElse(""))

      appContext.setAMContainerSpec(amContainer)
      client.submitApplication(appContext)

      Some(appId)
    } catch {
      case err: Exception =>
        logger.error(err.toString)
        None
    }

  def buildCredentials(appId: ApplicationId): Credentials = {
    val cred = new Credentials()
    val token = client.getAMRMToken(appId)
    cred.addToken(token.getService, token)
    cred
  }

  def runWithCredentials[A](fn: => A, cred: Credentials): A = {
    val prop = System.getProperty("user.name")
    val name = if (prop == null) DEFAULT_USER else prop
    val ugi = UserGroupInformation.createRemoteUser(name)
    ugi.addCredentials(cred)
    ugi.doAs(new PrivilegedExceptionAction[A]() {
      override def run: A = fn
    })
  }

  /** Fetches the current state of the app on YARN
    * @param id ID of the YARN app
    * @return Current State
    */
  def getAppStatus(id: ApplicationId): Option[YarnApplicationState] = {
    try {
      Some(client.getApplicationReport(id).getYarnApplicationState)
    } catch {
      case err: Exception =>
        logger.error(err.toString)
        None
    }
  }

  def isAccepted(applicationId: ApplicationId): Boolean = {
    try {
      val state = client.getApplicationReport(applicationId).getYarnApplicationState
      if (state == YarnApplicationState.ACCEPTED) true else false
    } catch {
      case err: Exception =>
        false
    }
  }


}
