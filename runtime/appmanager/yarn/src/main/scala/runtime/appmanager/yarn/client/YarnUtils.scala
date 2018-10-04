package runtime.appmanager.yarn.client

import java.io.{BufferedOutputStream, File, FileOutputStream, InputStream}

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.records.{LocalResource, LocalResourceType, LocalResourceVisibility}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.{Apps, ConverterUtils, Records}
import runtime.appmanager.yarn.util.YarnAppmanagerConfig

import scala.util.Try

private[yarn] object YarnUtils extends YarnAppmanagerConfig with LazyLogging {

  /** Distributes a binary to HDFS
    *
    * @param appId Arc App ID
    * @param localFilePath path to local binary
    * @return Option of the Destination path
    */
  def moveToHDFS(appId: String, taskName: String, localFilePath: String): Option[Path] = {
    val conf = new YarnConfiguration()

    val destFs = FileSystem.get(conf)

    createDirectories(destFs, appId)

    val destPath = destFs.makeQualified(new Path(appsDir+"/"+appId+"/"+ taskName))

    if (destFs.exists(destPath)) {
      logger.error("copyToHDFS: Destination path already exists: " + destPath.toString)
      None
    } else {
      logger.info(s"Transfering $localFilePath to $destPath")
      try {
        // False means to not remove the file from the local system
        // Which we might wanna change to true in the future
        destFs.copyFromLocalFile(false, new Path(localFilePath),  destPath)
        Some(destPath)
      } catch {
        case err: Exception =>
          logger.error(err.toString)
          None
      }
    }
  }

  /** Helper Method to create directory for a certain app
    * in HDFS if it does not exist already.
    * @param destFs Hadoop FileSystem
    * @param appId Id of ArcApp
    */
  private def createDirectories(destFs:FileSystem, appId: String): Unit = {
    val appRootPath = destFs.makeQualified(new Path(appsDir))
    if (!destFs.exists(appRootPath))
      destFs.mkdirs(appRootPath)

    val appsDirPath = destFs.makeQualified(new Path(appsDir, appId))
    if (!destFs.exists(appsDirPath))
      destFs.mkdirs(appsDirPath)
  }

  /** Deletes a apps directory on HDFS,
    * i.e., removes the binaries after the app  has
    * exited/finished.
    * @param appId ID of the app
    * @return true on success, otherwise false
    */
  def cleanApp(appId: String): Boolean = {
    Try {
      val conf = new YarnConfiguration()
      val fs = FileSystem.get(conf)
      val dirPath = new Path(appsDir + "/" + appId)
      if (fs.exists(dirPath))
        fs.delete(dirPath, true)
    }.isSuccess
  }


  def setLocalResource(path: Path, conf: YarnConfiguration): LocalResource = {
    val stat = FileSystem.get(conf).getFileStatus(path)
    val resource = Records.newRecord(classOf[LocalResource])
    resource.setSize(stat.getLen)
    resource.setResource(ConverterUtils.getYarnUrlFromPath(path)) // Check
    resource.setTimestamp(stat.getModificationTime)
    resource.setType(LocalResourceType.FILE)
    resource.setVisibility(LocalResourceVisibility.PUBLIC)

    resource
  }

  /** Set's the Environment for a
    * ContainerLaunchContext
    * @param conf YarnConfiguration
    * @return Environment Map
    */
  def setEnv(conf: YarnConfiguration): Map[String, String] = {
    import collection.JavaConverters._
    val classpath = conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH, YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH: _*)
    val envMap = new java.util.HashMap[String, String]()
    classpath.foreach(c => Apps.addToEnvironment(envMap, Environment.CLASSPATH.name(), c.trim, File.pathSeparator))

    Apps.addToEnvironment(envMap, Environment.CLASSPATH.name(), Environment.PWD.$() + File.pathSeparator + "*", File.pathSeparator)
    envMap.asScala.toMap
  }

}
