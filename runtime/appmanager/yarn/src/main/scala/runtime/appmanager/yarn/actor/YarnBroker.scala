package runtime.appmanager.yarn.actor

import java.nio.ByteBuffer
import java.util

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable, Props, Terminated}
import akka.util.Timeout
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.client.api.async.{AMRMClientAsync, NMClientAsync}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.Records
import runtime.appmanager.core.actor.ResourceBroker
import runtime.appmanager.yarn.client.{Client, YarnExecutor, YarnUtils}
import runtime.appmanager.yarn.util.YarnAppmanagerConfig
import runtime.common.Identifiers
import runtime.protobuf.ExternalAddress
import runtime.protobuf.messages.{Container => _, _}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future
import scala.concurrent.duration._


private[yarn] object YarnBroker {
  def apply(app: ArcApp, stateMasterConn: StateMasterConn): Props =
    Props(new YarnBroker(app, stateMasterConn))
  final case class ContainerRecord(allocId: Long, containerId: Long, container: Container)

  final case class YarnAcceptedCheck(appId: ApplicationId)
  final case class ContainerCompleted(containerId: Long)
  final case class ContainerStarted(containerRecord: ContainerRecord)
  final case class ContainersAllocated(containers: Seq[Container])
  final case class DeployableContainer(executorRef: ActorRef, container: ContainerRecord, info: ContainerInfo)
  final case class AllocRequest(profiles: Seq[ResourceProfile])
  final case object BrokerReady
}


private[yarn] class YarnBroker(app: ArcApp, stateMasterConn: StateMasterConn)
  extends ResourceBroker with YarnAppmanagerConfig {
  import YarnBroker._

  // Client for creating YARN AM etc..
  private val yarnConf = new YarnConfiguration()
  private val yarnClient = new Client(yarnConf)

  // Clients for handling containers on YARN
  private var rmClient: AMRMClientAsync[ContainerRequest] = _
  private var nmClient: NMClientAsync = _

  // Helpers for YARN AM acceptance
  private var acceptedTicker: Option[Cancellable] = None
  private val acceptCheckMs: Long = 500
  private var acceptedAttempts = 0
  private val acceptedMaxAttempts = 5

  // Available resources from the ResourceManager
  private var maxMemory: Long = -1
  private var maxCores: Int = -1

  // Container state
  private var completedContainers = ArrayBuffer.empty[ContainerRecord]
  private var activeContainers = ArrayBuffer.empty[ContainerRecord]
  private var pendingContainers = ArrayBuffer.empty[ContainerRecord]
  private var allocID: Int = 0

  import runtime.protobuf.ProtoConversions.ActorRef._
  implicit val sys = context.system

  private val statemaster: ActorRef = stateMasterConn.ref
  private val appmaster = context.parent

  private val stateMasterStr = statemaster.path
    .toStringWithAddress(statemaster.path.address)

  private val selfAddr = ExternalAddress(context.system).addressForAkka
  private val brokerPath = self.path.
    toStringWithAddress(selfAddr)

  implicit val timeout = Timeout(2 seconds)

  override def preStart(): Unit = {
    val appIdOpt = yarnClient.launchUnmanagedAM(Some(app.id))
    appIdOpt match {
      case Some(appId) =>
        acceptedTicker = startAcceptTicker(appId, self)
      case None =>
        log.error("Was not able to launch YARN AM")
      // Shut down?
    }
  }

  private def startAcceptTicker(appId: ApplicationId, myself: ActorRef): Option[Cancellable] = {
    implicit val ec = context.dispatcher
    Some(context.
      system.scheduler.schedule(
      0.milliseconds,
      acceptCheckMs.milliseconds) {
      myself !  YarnAcceptedCheck(appId)
    })
  }


  override def postStop(): Unit = {
    // Clean resources
    shutdown(FinalApplicationStatus.ENDED, "Closing down")
  }

  def receive = {
    case YarnAcceptedCheck(id) if yarnClient.isAccepted(id) =>
      acceptedTicker.foreach(_.cancel())
      val credentials = yarnClient.buildCredentials(id)
      yarnClient.runWithCredentials(registerAM(id), credentials)
    case YarnAcceptedCheck(id) =>
      acceptedAttempts += 1
      if (acceptedAttempts >= acceptedMaxAttempts)
        yarnAcceptFailure()
    case YarnExecutorUp(containerId, info) =>
      context watch sender() // Enable DeathWatch on Executor
      activeContainers.find(_.containerId == containerId) match {
        case Some(container) =>
          context.parent ! DeployableContainer(sender(), container, info)
        case None =>
      }
    case ContainerCompleted(id) =>
      activeContainers.find(_.containerId == id) match {
        case Some(container) =>
          completedContainers += container
          activeContainers = activeContainers.filterNot(_.containerId == id)
        case None =>
      }
    case ContainerStarted(container) =>
      pendingContainers -= container
      activeContainers += container
    case ContainersAllocated(containers) =>
      startExecutorContainer(containers)
    case Terminated(ref) =>
      // Executor has died. Act accordingly
    case AllocRequest(profiles) =>
      profiles.foreach { profile =>
        allocateContainer(profile, allocID)
        allocID += 1
      }
    case _ =>
  }

  private def yarnAcceptFailure(): Unit = {
    acceptedTicker.foreach(_.cancel())
    log.error("Was not able to get accepted by YARN, shutting down!")
    context stop self
  }

  private def registerAM(appId: ApplicationId): Unit = {
    try {
      rmClient = AMRMClientAsync.createAMRMClientAsync[ContainerRequest](AMRMHeartbeatInterval, AMRMHandler)
      rmClient.init(yarnConf)
      rmClient.start()

      nmClient = NMClientAsync.createNMClientAsync(NMHandler)
      nmClient.init(yarnConf)
      nmClient.start()

      log.info("Registering ApplicationMaster")
      val res = rmClient.registerApplicationMaster(appId.toString, -1, "")
      maxMemory = res.getMaximumResourceCapability.getMemorySize
      maxCores = res.getMaximumResourceCapability.getVirtualCores

      // Notify AppMaster that the Broker is ready to to request resources
      appmaster ! BrokerReady
    } catch {
      case err: Exception =>
        log.error(err.toString)
    }
  }


  private def allocateContainer(profile: ResourceProfile, allocId: Int): Unit = {
    val resource = Resource.newInstance(profile.memoryInMb, profile.cpuCores)
    val priority = Records.newRecord(classOf[Priority])
    // hardcoded for now
    priority.setPriority(1)
    rmClient.addContainerRequest(new ContainerRequest(resource, null, null, priority, allocId))
  }

  /** Container(s) have been allocated, now tasks can
    * be launched onto them.
    * @param containers YARN containers
    */
  private def startExecutorContainer(containers: Seq[Container]): Unit = {
    containers.foreach { container =>
      val allocId = container.getAllocationRequestId
      val containerId = container.getId.getContainerId
      val ctx = YarnExecutor.context(brokerPath, stateMasterConn.ref.path,
        stateMasterConn.kompactProxyAddr, app.id, containerId)

      nmClient.startContainerAsync(container, ctx)
      self ! ContainerStarted(ContainerRecord(allocId, containerId, container))
    }
  }

  private def shutdown(status: FinalApplicationStatus, message: String): Unit = {
    rmClient.unregisterApplicationMaster(status, message, null)
    rmClient.stop()
    nmClient.stop()

    if (YarnUtils.cleanApp(app.id))
      log.info(s"Cleaned HDFS directory for app")
    else
      log.error(s"Was not able to clean the HDFS directory for app")


    status match {
      case FinalApplicationStatus.FAILED =>
        appmaster ! TaskMasterStatus(Identifiers.ARC_APP_FAILED)
      case FinalApplicationStatus.KILLED =>
        appmaster ! TaskMasterStatus(Identifiers.ARC_APP_KILLED)
      case FinalApplicationStatus.SUCCEEDED =>
        appmaster ! TaskMasterStatus(Identifiers.ARC_APP_SUCCEEDED)
      case FinalApplicationStatus.ENDED =>
        appmaster ! TaskMasterStatus(Identifiers.ARC_APP_SUCCEEDED)
      case FinalApplicationStatus.UNDEFINED =>
        appmaster ! TaskMasterStatus(Identifiers.ARC_APP_FAILED)
    }

    // Shut down the actor
    context stop self
  }


  // ApplicationMaster ResourceManager Client Async Callbacks

  import scala.collection.JavaConverters._

  private def AMRMHandler: AMRMClientAsync.AbstractCallbackHandler =
    new AMRMClientAsync.AbstractCallbackHandler {
      override def onContainersAllocated(containers: util.List[Container]): Unit =
        self ! ContainersAllocated(containers.asScala)
      override def onContainersCompleted(list: util.List[ContainerStatus]): Unit = {
        list.asScala.foreach {container =>
          self ! ContainerCompleted(container.getContainerId.getContainerId)
        }
      }
      override def onContainersUpdated(list: util.List[UpdatedContainer]): Unit =
        log.info("On containers updated: $list")
      override def onShutdownRequest(): Unit =
        shutdown(FinalApplicationStatus.FAILED, "KILLED")
      override def getProgress: Float = 100
      override def onNodesUpdated(list: util.List[NodeReport]): Unit = {}
      override def onError(throwable: Throwable): Unit = {
        log.info("Error: " + throwable.toString)
        shutdown(FinalApplicationStatus.FAILED, throwable.toString)
      }
    }

  // NodeManager Async client Callbacks

  private def NMHandler: NMClientAsync.AbstractCallbackHandler = new NMClientAsync.AbstractCallbackHandler {
    override def onGetContainerStatusError(containerId: ContainerId, throwable: Throwable): Unit =
      log.error(s"Container StatusError: $containerId with ${throwable.toString}")
    override def onContainerStatusReceived(containerId: ContainerId, containerStatus: ContainerStatus): Unit =
      log.info(s"Container status received for $containerId with $containerStatus")
    override def onContainerResourceIncreased(containerId: ContainerId, resource: Resource): Unit =
      log.info("Container resoruce increased")
    override def onStopContainerError(containerId: ContainerId, throwable: Throwable): Unit = {
      log.error(s"Error container stopped: $containerId with reason ${throwable.toString}")
    }
    override def onContainerStopped(containerId: ContainerId): Unit = {
      log.error(s"Container stopped $containerId")
    }
    override def onIncreaseContainerResourceError(containerId: ContainerId, throwable: Throwable): Unit = {}
    override def onStartContainerError(containerId: ContainerId, throwable: Throwable): Unit = {
      log.error(s"On Start Container error for $containerId with ${throwable.toString}")
    }
    override def onUpdateContainerResourceError(containerId: ContainerId, throwable: Throwable): Unit =
      log.error(s"On Update resource error for $containerId with ${throwable.toString}")

    override def onContainerStarted(containerId: ContainerId, map: util.Map[String, ByteBuffer]): Unit = {
      //self ! ContainerStarted()
      log.info(s"Container $containerId started with $map")
    }
    override def onContainerResourceUpdated(containerId: ContainerId, resource: Resource): Unit =
      log.info("container resource updated")
  }


  // TODO
  override def startExecutor(): Unit = ???

}
