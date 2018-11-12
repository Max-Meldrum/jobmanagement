package runtime.worker

import akka.actor.{Actor, ActorLogging, Props}

private[worker] object Worker {
  def apply(): Props = Props(new Worker)
}

private[worker] class Worker extends Actor with ActorLogging {

  override def receive: Receive = {
    case _ =>
  }
}
