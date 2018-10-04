package executor.yarn

import executor.common.{Linux, OperatingSystem}
import runtime.protobuf.messages.ContainerInfo

private[yarn] object ExecutorDetails {
  import sys.process._

  def gather(): Option[ContainerInfo] = OperatingSystem.get() match {
    case Linux =>
      uname()
    case _ =>
      None
  }

  /** Fetches
    * 1. Machine Name
    * 2. Hardware type
    * 3. OS type
    * @return
    */
  private def uname():  Option[ContainerInfo] = {
    try {
      val res = "uname -i -o -n".!!
      val split = res.split(" ")
      if (split.size == 3)
        Some(ContainerInfo(split(0), split(1), split(2)))
      else
        None
    } catch {
      case err: Exception =>
        None
    }
  }
}
