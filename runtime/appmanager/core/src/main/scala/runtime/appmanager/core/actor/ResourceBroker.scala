package runtime.appmanager.core.actor

import akka.actor.{Actor, ActorLogging}

private[appmanager] trait ResourceBroker extends Actor with ActorLogging {
  def startExecutor(): Unit
}


