package executor.yarn

import java.nio.file.Paths

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable, Props, Terminated}
import executor.common.{ExecutionEnvironment, ExecutorStats}
import runtime.common.Identifiers
import runtime.protobuf.ExternalAddress

import scala.util.Try
import runtime.protobuf.messages._

import scala.concurrent.Future
import scala.concurrent.duration._

private[yarn] object Executor {
  def apply(appId: String,
            containerId: Int,
            resourceBroker: ActorRef,
            stateMaster: ActorRef,
            stateMasterKompactAddr: String): Props =
    Props(new Executor(appId, containerId, resourceBroker, stateMaster, stateMasterKompactAddr))
  final case object HealthCheck
  final case class StdOutResult(r: String)
  final case class CreateTaskReader(task: ArcTask)
}

/** Actor that is responsible for executing
  * and monitoring a binary
  * @param appId ArcApp Id
  * @param containerId ID of the YARN container
  * @param resourceBroker ActorRef of ResourceBroker
  * @param stateMaster ActorRef of StateMaster
  * @param stateMasterKompactAddr Addr to KompactProxy
  */
private[yarn] class Executor(appId: String,
                             containerId: Int,
                             resourceBroker: ActorRef,
                             stateMaster: ActorRef,
                             stateMasterKompactAddr: String,
                            ) extends Actor with ActorLogging with ExecutorConfig {

  private var healthChecker = None: Option[Cancellable]
  private var process = None: Option[Process]
  private var monitor = None: Option[ExecutorStats]
  private var arcTask = None: Option[ArcTask]

  private val selfAddr = self.path.
    toStringWithAddress(ExternalAddress(context.system).addressForAkka)

  // TODO: handle creation/deletion/failures of env
  private val env = new ExecutionEnvironment(appId)

  import Executor._
  import context.dispatcher

  override def preStart(): Unit = {
    if (env.create().isSuccess) {
      ExecutorDetails.gather() match {
        case Some(info) =>
          resourceBroker ! YarnExecutorUp(containerId, info)
        case None =>
          resourceBroker ! "fail?" // fix
      }
    } else {
      resourceBroker ! "environment creation failed" // fix
    }
  }

  override def postStop(): Unit = {
    env.clean()
  }

  def receive = {
    case ExecutorBinary(hdfsPath) =>
      loadBinary(hdfsPath)
    case HealthCheck =>
      monitor match {
        case Some(stats) =>
          collectMetrics(stats)
        case None =>
          log.info("Could not load monitor")
          shutdown()
      }
    case ArcTaskUpdate(t) =>
      // Gotten the results from the Stdout...
      arcTask = Some(t)
    case Terminated(sMaster) =>
      log.info(s"Lost Contact with our StateMaster $stateMaster")
      // Handle?
    case _ =>
  }

  private def loadBinary(hdfsPath: String): Future[Unit] = Future {
    val binName = Paths.get(hdfsPath)
      .getFileName
      .toString

    val binPath = env.getAppPath + "/" + binName
    if (ExecutorUtils.moveToLocal(hdfsPath, binPath)) {
      log.info("Binary moved to local environment, setting as executable!")
      env.setAsExecutable(binPath)
      execute(binPath, binName)
    } else {
      log.error("Failed to move binary to local")
    }
  }

  /** Executes @binPath and initializes the monitoring service
    */
  private def execute(binPath: String, binName: String): Unit = {
    val stateManagerProxy = stateMasterKompactAddr
    val stateMasterPath = stateMaster.path
      .toSerializationFormatWithAddress(ExternalAddress(context.system).addressForAkka)

    val pb = new ProcessBuilder(binPath, binName, stateManagerProxy, stateMasterPath ,stateManagerProxy, stateMasterPath)

    if (!logSetup(binName, pb))
      shutdown()

    process = Some(pb.start())

    val p = getPid(process.get)
      .toOption

    p match {
      case Some(pid) =>
        ExecutorStats(pid, binPath, selfAddr) match {
          case Some(execStats) =>
            monitor = Some(execStats)
            healthChecker = scheduleCheck()
            // Enable DeathWatch of the StateMaster
            context watch stateMaster
          case None =>
            log.error("Was not able to create ExecutorStats instance")
            shutdown()
        }
      case None =>
        log.error("Executor.getPid() requires an UNIX system")
        shutdown()
    }
  }

  private def logSetup(binName: String, pb: ProcessBuilder): Boolean = {
    // Create log file
    if (env.createLogForTask(binName)) {
      // Set up logging
      env.getLogFile(binName).map(_.toFile) match {
        case Some(file) =>
          pb.redirectOutput(file)
          pb.redirectError(file)
          true
        case None =>
          log.error(s"Failed to set up logging for binary $binName")
          false
      }
    } else {
      false
    }
  }

  /** Uses ExecutorStats to gather process metrics
    * to then send to a StateMaster
    * @param stats ExecutorStats instance
    */
  private def collectMetrics(stats: ExecutorStats): Unit = {
   if (process.isDefined && process.get.isAlive) {
     stats.complete() match {
       case Left(metric) =>
         //stateMaster ! ArcTaskMetric(arcTask.get, metric)
         println(metric)
       case Right(err) =>
         log.error(err.toString)
     }
   } else {
     log.info("Process is no longer alive, shutting down!")
     arcTask match {
       case Some(t) =>
         val updatedTask = t.copy(status = Some(Identifiers.ARC_TASK_KILLED))
         //appMaster ! ArcTaskUpdate(updatedTask)
       case None =>
     }
     shutdown()
   }
  }


  /**
    * Only works on Unix based systems. Java 9 Process API has a
    * getPid() Method but we are limited to Java 8.
    */
  private def getPid(p: Process): Try[Long] = Try {
    val field = p.getClass.getDeclaredField("pid")
    field.setAccessible(true)
    field.get(p)
      .toString
      .toLong
  }

  private def scheduleCheck(): Option[Cancellable] = {
    Some(context.system.scheduler.schedule(
      monitorInterval.milliseconds,
      monitorInterval.milliseconds,
      self,
      HealthCheck
    ))
  }

  /**
    * Stop the health ticker and instruct the actor to close down.
    */
  private def shutdown(): Unit = {
    log.info("Shutting down Executor")
    healthChecker.map(_.cancel())
    context.system.terminate()
  }

}
