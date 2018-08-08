package clustermanager.standalone.taskmanager.isolation

import java.io.IOException
import java.nio.file.{Files, Path, Paths, StandardOpenOption}

import clustermanager.common.{Linux, OperatingSystem}
import clustermanager.standalone.taskmanager.utils.ContainerUtils
import com.typesafe.scalalogging.LazyLogging
import runtime.protobuf.messages.Container



/** LinuxContainerEnvironment uses Cgroups to devide and isolate
  * resources such as CPU and Memory for each Container.
  * @param availableCores cores available to the containers Cgroup
  * @param availableMem memory available to the containers Cgroup
  */
class LinuxContainerEnvironment(availableCores: Int, availableMem: Long)
  extends Cgroups {
  import LinuxContainerEnvironment._

  /** Checks if Cgroups has been set up correctly.
    * Throws a CgroupsException if a fault is found.
    */
  private[LinuxContainerEnvironment] def check(): Unit = {
    if (OperatingSystem.get() != Linux)
      throw CgroupsException("Cgroups requires a Linux Distribution")

    if (!Files.isDirectory(Paths.get(defaultPath)))
      throw CgroupsException(s"Cgroups is not mounted at $defaultPath")


    val userDirs  = Seq(containersCpu, containersMem)

    userDirs.foreach { dir =>
      if (!Files.isDirectory(Paths.get(dir)))
        throw CgroupsException(s"Cannot find Cgroup $dir")

      if (!Files.isWritable(Paths.get(dir)))
        throw CgroupsException(s"Missing Write permissions to $dir")

      if (!Files.isReadable(Paths.get(dir)))
        throw CgroupsException(s"Missing Read permissions to $dir")
    }
  }


  /** Initializes cpu/containers and memory/containers.
    * Sets hard limits in order to ensure resources for other services
    * running on the OS.
    */
  def initRoot(): Unit = {
    // Setting Hard Limits for CPU cores:
    // Is done by giving values to cpu.cfs_quota_us and cpu.cfs_period_us
    // Example: enable full utilization of 4 CPU cores
    // cpu.cfs_quota_us = 400000, cpu.cfs_period_us = 100000

    // Period is by default 100000 on my system, check so that we don't need to actually write this down as well.
    val period = 100000
    val quota = period * availableCores
    Files.write(Paths.get(containersCpu + "/" + CPU_CFS_QUOTA), String.valueOf(quota).getBytes)

    // Setting hard limits for Memory usage:
    // Is done by giving a value to memory.limit_in_bytes
    Files.write(Paths.get(containersMem + "/" + MEMORY_LIMIT), String.valueOf(availableMem).getBytes)

    // Just an extra verification
    assert(readToLong(Paths.get(containersCpu + "/" + CPU_CFS_QUOTA)).isDefined)
    assert(readToLong(Paths.get(containersMem + "/" + MEMORY_LIMIT)).isDefined)
  }

  /** Creates a Container Group for the specified
    * container. Throws CgroupsException on Error.
    * @param id Container ID
    */
  def createContainerGroup(id: String, container: Container): Unit = {
    val cpuPath = Paths.get(containersCpu + "/" + id)
    val memPath = Paths.get(containersMem + "/" + id)

    if (Files.isDirectory(memPath))
      throw CgroupsException(s"Container already exists under $containersMem")

    if (Files.isDirectory(cpuPath))
      throw CgroupsException(s"Container already exists under $containersCpu")

    Files.createDirectory(memPath)
    Files.createDirectory(cpuPath)

    // Just an extra check
    if (!(Files.isDirectory(memPath) || Files.isDirectory(cpuPath)))
      throw CgroupsException("Something went wrong while trying to create Container Group")

    // Set Soft limit for the Container groups CPU usage
    // cpu.shares = 1024 * (total CPU cores in container / Total available CPU cores in cpu/containers)
    // e.g., 1024 * (2/4) = 512 cpu shares
    val containerCores = container.tasks.foldLeft(0)(_ + _.cores)
    val shares = 1024 * (containerCores / availableCores)
    logger.info(s"Setting Container group CPU shares to $shares")
    Files.write(Paths.get(cpuPath + "/" + CPU_SHARES), String.valueOf(shares).getBytes)

    // For now, it is enough by having all the binaries in a job be moved to a single container group.
    // Later on child groups can be created with soft limits for each task.
  }


  def createController(containerId: String): CgroupController =
    new CgroupController(containerId)

  def shutdown(): Unit = {
    // Check if there are any containers, if so try to clean them.
  }

  def getCores: Int = availableCores
  def getMemory: Long = availableMem
}

private[taskmanager] object LinuxContainerEnvironment extends LazyLogging {
  case class CgroupsException(msg: String) extends Exception

  def apply(): Either[Exception, LinuxContainerEnvironment] =  {
    val availableCores = ContainerUtils.getNumberOfContainerCores
    val availableMem = ContainerUtils.getMemoryForContainers
    val env = new LinuxContainerEnvironment(availableCores, availableMem)
    try {
      env.check() // Verify Cgroups Setup
      env.initRoot()
      Right(env)
    } catch {
      case err: CgroupsException =>
        logger.error(err.msg)
        Left(err)
      case ioErr: IOException =>
        logger.error(ioErr.toString)
        Left(ioErr)
    }
  }
}

/** Used by TaskMaster's and TaskExecutor's to
  * handle the Containers Cgroup.
  * @param containerId ID for the Container
  */
class CgroupController(containerId: String) extends Cgroups {
  private val reporter = new LCEReporter(containerId)
  private final val containerCpu = containersCpu + "/" + containerId
  private final val containerMem = containersMem + "/" + containerId


  /** Move process into cgroup by writing PID into the
    * groups cgroup.procs file.
    * @param pid Process which we are moving
    * @return True on success, false otherwise
    */
  def mvProcessToCgroup(pid: Long): Boolean = {
    // Double check directories exist..

    // write pid into containerId/tasks file
    Files.write(Paths.get(containerCpu + "/" + CGROUP_PROCS) , String.valueOf(pid).getBytes,
      StandardOpenOption.APPEND)
    Files.write(Paths.get(containerMem + "/" + CGROUP_PROCS), String.valueOf(pid).getBytes,
      StandardOpenOption.APPEND)
    true
  }

  /** Deletes the Container cgroup under
    * cpu/containers/ and memory/containers/
    */
  def clean(): Unit =
    deleteContainerGroup(containerId)
}

trait Cgroups extends LazyLogging {

  // For now. Path on RHEL based systems might be found at /cgroup etc..
  final val defaultPath = "/sys/fs/cgroup"

  // Cgroup for containers started by the TaskManager
  final val containersCpu = defaultPath + "/" + "cpu/containers"
  final val containersMem = defaultPath + "/" + "memory/containers"

  // Identifiers
  final val MEMORY_LIMIT = "memory.limit_in_bytes"
  final val MEMORY_USAGE = "memory.usage_in_bytes"
  final val CPU_SHARES = "cpu.shares"
  final val CPU_CFS_QUOTA = "cpu.cfs_quota_us"
  final val CPU_CFS_PERIOD = "cpu.cfs_period_us"
  final val CGROUP_PROCS = "cgroup.procs"


  /** Used to read files with one line values
    * @param path Path to file
    * @return Option[Long]
    */
  protected def readToLong(path: Path): Option[Long] = {
    try {
      val str = Files.lines(path)
        .findFirst()
        .get()

      Some(str.toLong)
    } catch {
      case err: Exception =>
        logger.error(err.toString)
        None
    }
  }

  /** Deletes a Container's Cgroup
    * Should be called on shutdown of TaskMaster
    * @param id Container ID
    * @return true on success, otherwise false
    */
  protected def deleteContainerGroup(id: String): Boolean = {
    val cpuPath = Paths.get(containersCpu + "/" + id)
    val memPath = Paths.get(containersMem + "/" + id)
    Files.deleteIfExists(cpuPath) && Files.deleteIfExists(memPath)
  }

  def getRootMemoryLimit: Option[Long] =
    readToLong(Paths.get(defaultPath + "/" + "memory" + "/" +  MEMORY_LIMIT))

  def getMemoryLimit(containerId: String): Option[Long] =
    readToLong(Paths.get(containersMem + "/" + containerId + "/" + MEMORY_LIMIT))

  def setMemoryLimit(value: Long, containerId: String): Unit = {
    Files.write(Paths.get(containersMem + "/" + containerId + "/" + MEMORY_LIMIT),
      String.valueOf(value).getBytes())
  }
}

class LCEReporter(containerId: String) extends Cgroups {
  // Fetch container metrics
}


