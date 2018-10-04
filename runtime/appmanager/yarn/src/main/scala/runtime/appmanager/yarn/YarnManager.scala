package runtime.appmanager.yarn

import akka.actor.ActorSystem
import com.typesafe.scalalogging.LazyLogging
import actor.YarnAppmanager
import com.typesafe.config.ConfigFactory
import runtime.appmanager.yarn.util.YarnAppmanagerConfig
import runtime.common.Identifiers

object YarnManager extends App with LazyLogging with YarnAppmanagerConfig {
  logger.info("Starting up YARN AppManager")
  val system = ActorSystem(Identifiers.CLUSTER, ConfigFactory.load())
  val appmanager = system.actorOf(YarnAppmanager(), Identifiers.APP_MANAGER)
  system.whenTerminated
}
