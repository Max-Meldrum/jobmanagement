package runtime.appmanager.yarn.actor

import akka.actor.{ActorRef, Props}
import akka.cluster.Cluster
import runtime.appmanager.core.actor.AppMaster
import runtime.appmanager.core.util.Ascii.{LogicalDAG, PhysicalDAG}
import runtime.appmanager.core.util.{Ascii, DagDrawer}
import runtime.appmanager.yarn.actor.YarnBroker.{AllocRequest, BrokerReady, DeployableContainer}
import runtime.appmanager.yarn.client.YarnUtils
import runtime.protobuf.messages.{ArcApp, ExecutorBinary, ResourceProfile, StateMasterConn}

import scala.concurrent.Future


private[yarn] object YarnAppMaster {
  def apply(app: ArcApp): Props = Props(new YarnAppMaster(app))
}

/**
  * This class is not supposed to represent a YARN ApplicationMaster
  */
private[yarn] class YarnAppMaster(app: ArcApp) extends AppMaster {
  import AppMaster._
  import runtime.appmanager.core.actor.AppManager._

  private val selfAddr = Cluster(context.system).selfAddress

  // Handles implicit conversions of ActorRef and ActorRefProto
  import runtime.protobuf.ProtoConversions.ActorRef._

  implicit val ec = context.dispatcher

  private var broker: Option[ActorRef] = None

  override def preStart(): Unit = {
    super.preStart()
    arcApp = Some(app)
    self ! PrintEvent(AppEvent(s"AppMaster for App ${app.id} starting up"))
    Ascii.createDAGHeader(LogicalDAG, self)
    self ! PrintEvent(AppEvent(DagDrawer.logicalDAG()))
    Ascii.createDAGHeader(PhysicalDAG, self)
    self ! PrintEvent(AppEvent(DagDrawer.phyiscalDAG()))
  }

  override def receive: Receive = super.receive orElse {
    case conn@StateMasterConn(ref, kompactProxyAddr) =>
      stateMasterConn = Some(conn)
      broker = Some(context.actorOf(YarnBroker(app, conn), "resourcebroker"))
    case StateMasterError =>
      log.error("Error while fetching StateMaster")
      publisherRef ! AppEvent("Failed to fetch StateMaster")
      // Retry fetching statemaster or shut down
    case DeployableContainer(executor, container, info) =>
      val res = container.container.getResource
      val containerCores = res.getVirtualCores
      val containerMemory = res.getMemorySize
      self ! PrintEvent(AppEvent(s"Executor ready for deployment $executor with resource $containerCores cores and $containerMemory memory"))
      transferFile("executor" + container.allocId, executor)
    case BrokerReady =>
      val profiles = Seq(ResourceProfile(1, 1024), ResourceProfile(1, 1024))
      sender() ! AllocRequest(profiles)
  }


  private def transferFile(name: String, executor: ActorRef): Future[Unit] = Future {
    YarnUtils.moveToHDFS(app.id, name, "executor_rust") match {
      case Some(path) =>
        executor ! ExecutorBinary(path.toString)
      case None =>
        executor ! "Failed?"
    }
  }

}
