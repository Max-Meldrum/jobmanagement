package runtime.appmanager.core.actor


import akka.actor.{Actor, ActorLogging, ActorRef, Address, Props, Terminated}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{MemberRemoved, MemberUp, UnreachableMember}
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import akka.util.Timeout
import runtime.appmanager.core.rest.RestService
import runtime.appmanager.core.util.AppManagerConfig
import runtime.common.{ActorPaths, Identifiers}
import runtime.protobuf.messages._
import akka.pattern._
import runtime.appmanager.core.actor.MetricAccumulator.{StateManagerMetrics, TaskManagerMetrics}

import scala.concurrent.duration._
import scala.collection.mutable
import scala.concurrent.Future
import scala.util.Try


private[appmanager] object AppManager {
  final case class ArcAppRequest(app: ArcApp, liveFeed: Boolean = false)
  final case class ArcDeployRequest(priority: Int, locality: Boolean, tasks: Seq[ArcTask])
  final case class ArcAppStatus(id: String)
  final case class KillArcAppRequest(id: String)
  final case object ResourceManagerUnavailable
  final case object ListApps
  final case object ListAppsWithDetails
  final case object StateMasterError
  final case object AppStream
  type ArcAppId = String
}

private[appmanager] abstract class AppManager extends
  Actor with ActorLogging with AppManagerConfig {
  import AppManager._
  // For Akka HTTP (REST)
  protected implicit val materializer = ActorMaterializer()
  protected implicit val system = context.system
  protected implicit val ec = context.system.dispatcher

  // Timeout for Futures
  protected implicit val timeout = Timeout(2.seconds)

  // Fields
  protected var appMasters = mutable.IndexedSeq.empty[ActorRef]
  protected var appMasterId: Long = 0 // unique id for each AppMaster that is created
  protected var appMap = mutable.HashMap[ArcAppId, ActorRef]()

  protected var stateManagers = mutable.IndexedSeq.empty[Address]
  protected var stateManagerReqs: Int = 0
  protected val metricAccumulator = context.actorOf(MetricAccumulator())

  override def preStart(): Unit = {
    cluster.subscribe(self, classOf[MemberUp], classOf[UnreachableMember], classOf[MemberRemoved])

    log.info("Starting up REST server at " + interface + ":" + restPort)
    val rest = new RestService(self)
    Http().bindAndHandle(rest.route, interface, restPort)
  }

  override def postStop(): Unit =
    cluster.unsubscribe(self)

  private val cluster = Cluster(context.system)

  def receive = {
    case ArcAppRequest(_, _) if stateManagers.isEmpty =>
      sender() ! "No StateManagers available"
    case kill@KillArcAppRequest(id) =>
      appMap.get(id) match {
        case Some(appMaster) =>
          appMaster forward kill
        case None =>
          sender() ! s"Could not locate AppMaster connected to $id"
      }
    case s@ArcAppStatus(id) =>
      appMap.get(id) match {
        case Some(appMaster) =>
          appMaster forward s
        case None =>
          sender() ! s"Could not locate AppMaster connected to $id"
      }
    case t@TaskManagerMetrics =>
      metricAccumulator forward t
    case s@StateManagerMetrics =>
      metricAccumulator forward s
    case r@ArcAppMetricRequest(id) =>
      appMap.get(id) match {
        case Some(appMaster) =>
          appMaster forward r
        case None =>
          sender() ! ArcAppMetricFailure("Could not locate the app on this AppManager")
      }
    case ListApps =>
      gatherApps() pipeTo sender()
    case Terminated(ref) =>
      // AppMaster was terminated somehow
      appMasters = appMasters.filterNot(_ == ref)
      appMap.find(_._2 == ref) map { m => appMap.remove(m._1) }
    case MemberUp(m) if m.hasRole(Identifiers.STATE_MANAGER) =>
      stateManagers = stateManagers :+ m.address
    case MemberRemoved(m, status) if m.hasRole(Identifiers.STATE_MANAGER) =>
      stateManagers = stateManagers.filterNot(_ == m.address)
    case UnreachableMember(m) if m.hasRole(Identifiers.STATE_MANAGER) =>
    // Handle
  }

  /** Lists all jobs to the user
    *
    * @return Future containing statuses
    */
  protected def gatherApps(): Future[Seq[ArcApp]] = {
    Future.sequence(appMap.map { app =>
      (app._2 ? ArcAppStatus(app._1)).mapTo[ArcApp]
    }.toSeq)
  }

  protected def getStateMaster(amRef: ActorRef, app: ArcApp): Future[StateMasterConn] = {
    val smAddr = stateManagers(stateManagerReqs % stateManagers.size)
    val smSelection = context.actorSelection(ActorPaths.stateManager(smAddr))
    import runtime.protobuf.ProtoConversions.ActorRef._
    smSelection ? StateManagerJob(amRef, app) flatMap {
      case s@StateMasterConn(_,_) => Future.successful(s)
    } recoverWith {
      case t: akka.pattern.AskTimeoutException => Future.failed(t)
    }
  }
}
