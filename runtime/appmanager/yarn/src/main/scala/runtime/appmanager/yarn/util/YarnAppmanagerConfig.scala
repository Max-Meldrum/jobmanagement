package runtime.appmanager.yarn.util

import com.typesafe.config.ConfigFactory

trait YarnAppmanagerConfig {
  val config = ConfigFactory.load("yarn")

  val appsDir = config.getString("yarn.apps-dir")

  val executorJarPath = config.getString("yarn.executor-jar-path")
  val executorJar = config.getString("yarn.executor-jar")
  val executorClass = config.getString("yarn.executor-main-class")

  val AMRMHeartbeatInterval = config.getInt("yarn.AMRMHeartbeatInterval")

  require(config.isResolved, "YarnConfig has not been resolved")

  require(AMRMHeartbeatInterval > 0, "Heartbeat interval between YARN's AM and RM has to be larger than 0")
  
  require(appsDir.nonEmpty, "HDFS apps directory is not defined")

  require(executorClass.nonEmpty, "Executor class is not defined")
  require(executorJar.nonEmpty, "Executor Jar is not defined")
  require(executorJarPath.nonEmpty, "Executor's Jar Path is not defined")
}
