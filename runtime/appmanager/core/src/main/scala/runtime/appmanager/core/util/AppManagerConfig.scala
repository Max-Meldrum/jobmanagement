package runtime.appmanager.core.util

import com.typesafe.config.{ConfigFactory, ConfigList}
import runtime.common.Identifiers

trait AppManagerConfig {
  val config = ConfigFactory.load()
  val appMasterKeepAlive = config.getLong("appmanager.appMasterKeepAlive")
  val restPort = config.getInt("appmanager.restPort")
  val interface = config.getString("appmanager.interface")
  val restVersion = config.getString("appmanager.restVersion")
  val resourcemanager = config.getString("appmanager.resourcemanager")

  require(config.isResolved)
  require(appMasterKeepAlive > 0)
  require(restPort >= 0 && restPort <= 65535)
  require(!interface.isEmpty)
  require(!restVersion.isEmpty)

  val roles: ConfigList = config.getList("akka.cluster.roles")
  require(roles.unwrapped().contains(Identifiers.APP_MANAGER), "AppManager role has not been set")

}
