package executor.yarn

import com.typesafe.config.ConfigFactory

private[yarn] trait ExecutorConfig {
  val config = ConfigFactory.load("executor.conf")
  val monitorInterval = config.getInt("monitor-interval")

  require(config.isResolved)
  require(monitorInterval > 0)
}
