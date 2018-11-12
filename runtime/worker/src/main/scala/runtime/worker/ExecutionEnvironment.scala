package runtime.worker

import java.nio.file.attribute.{PosixFilePermission, PosixFilePermissions}
import java.nio.file.{FileAlreadyExistsException, Files, Path, Paths}
import java.util.Comparator
import java.util.stream.Collectors

import com.typesafe.scalalogging.LazyLogging
import runtime.common.OperatingSystem

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

private[worker] class ExecutionEnvironment(appId: String) extends LazyLogging {
  import runtime.common.OperatingSystem._

  // For now. To be discussed
  private final val LINUX_DIR = System.getProperty("user.home") + "/arc"
  private final val MAC_OS_DIR = System.getProperty("user.home") + "/arc"

  private final val LINUX_APP_PATH = LINUX_DIR + "/" + appId
  private final val MAC_OS_APP_PATH = MAC_OS_DIR + "/" + appId

  /**
    * Create a directory where the app will execute
    * Path will depend on OS
    */
  def create(): Try[Boolean] = Try {
    OperatingSystem.get() match {
      case Linux =>
        createLinuxAppDir().isSuccess
      case Mac =>
        false
      case Windows => // We shouldn't really get here
        false
      case _ =>
        false
    }
  }


  /**
    * Creates a directory with the app's id.
    * If the environment does not exist, it will create it.
    */
  private def createLinuxAppDir(): Try[Boolean] = Try {
    if (Files.exists(Paths.get(LINUX_DIR))) {
      Files.createDirectories(Paths.get(LINUX_APP_PATH))
      true
    } else {
      createLinuxEnv() match {
        case Success(_) =>
          Files.createDirectories(Paths.get(LINUX_APP_PATH))
          true
        case Failure(e) =>
          logger.error("Could not create linux env")
          logger.error(e.toString)
          false
      }
    }
  }

  private def createLinuxEnv(): Try[Unit] = Try {
    Files.createDirectories(Paths.get(LINUX_DIR),
      PosixFilePermissions.asFileAttribute(
        PosixFilePermissions.fromString("rwxr-x---") //TODO: look into fitting permissions
      ))
  }

  /**
    * Writes bytes to a file in the execution environment.
    * Used by Standalone
    */
  def writeBinaryToFile(id: String, file: Array[Byte]): Boolean  = {
    OperatingSystem.get() match {
      case Linux =>
        Files.write(Paths.get(LINUX_APP_PATH+"/"+id), file)
        setAsExecutable(LINUX_APP_PATH+"/"+id) match {
          case Success(_) =>
            true
          case Failure(e) =>
            logger.error(e.toString)
            false
        }
      case Mac => false
      case _ => false
    }
  }

  /** Make a binary executable.
    * Compatible with UNIX/Linux
    * @param path Path to the binary
    * @return Scala Try
    */
  def setAsExecutable(path: String): Try[Unit] = Try {
    import scala.collection.JavaConverters._

    val perms: mutable.HashSet[PosixFilePermission] = mutable.HashSet()
    perms += PosixFilePermission.OWNER_EXECUTE
    perms += PosixFilePermission.OWNER_READ
    perms += PosixFilePermission.OWNER_WRITE
    Files.setPosixFilePermissions(Paths.get(path), perms.asJava)
  }

  /**
    * Delete resources tied to the ExecutionEnvironment
    */
  def clean(): Unit = {
    import scala.collection.JavaConverters._

    OperatingSystem.get() match {
      case Linux =>
        val toDelete = Files.walk(Paths.get(LINUX_APP_PATH))
          .sorted(Comparator.reverseOrder())
          .collect(Collectors.toList())
          .asScala

        toDelete.foreach(Files.deleteIfExists(_))
      case Mac =>
      //TODO
      case _ =>
    }
  }

  /** Return path to the current App's ExecutionEnvironment
    * @return Path in String
    */
  def getAppPath: String = OperatingSystem.get() match {
    case Linux => LINUX_APP_PATH
    case Mac => MAC_OS_APP_PATH
    case _ => ""
  }


  /** Create log file for binary being executed.
    * File is placed within the ExecutionEnvironment
    * @param taskName name in String
    * @return true on success, otherwise false
    */
  def createLogForTask(taskName: String): Boolean = {
    try {
      val path = Paths.get(getAppPath + "/" + taskName.concat(".log"))
      Files.createFile(path)
      true
    } catch {
      case err: FileAlreadyExistsException =>
        true
      case err: Exception =>
        logger.error(err.toString)
        false
    }
  }

  def getLogFile(taskName: String): Option[Path] = {
    val name = taskName.concat(".log")
    val log = Paths.get(getAppPath + "/" + name)
    Option(Files.exists(log)).flatMap(f => if (f) Some(log) else None)
  }

}
