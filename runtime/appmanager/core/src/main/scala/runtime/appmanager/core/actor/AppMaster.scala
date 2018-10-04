package runtime.appmanager.core.actor

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem}
import akka.util.{ByteString, Timeout}
import runtime.appmanager.core.util.{AppManagerConfig, Ascii, AsciiSettings, Garamond}
import runtime.common.Identifiers
import runtime.protobuf.messages._
import akka.pattern._
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import org.reactivestreams.Publisher

import scala.concurrent.duration._

private[appmanager] object AppMaster {
  final case object AppEvent  {
    def apply(msg: String): ByteString = ByteString(msg + "\n")
  }
  final case class PrintEvent(event: ByteString)
}

private[appmanager] abstract class AppMaster extends Actor
  with ActorLogging with AppManagerConfig {
  import AppManager._
  import AppMaster._
  protected var stateMasterConn = None: Option[StateMasterConn]
  protected var arcApp = None: Option[ArcApp]

  // For futures
  protected implicit val timeout = Timeout(2 seconds)
  import context.dispatcher

  // Handles implicit conversions of ActorRef and ActorRefProto
  implicit val sys: ActorSystem = context.system
  import runtime.protobuf.ProtoConversions.ActorRef._

  implicit val mat = ActorMaterializer()

  protected val (publisherRef: ActorRef, publisher: Publisher[ByteString]) =
    Source.actorRef[ByteString](bufferSize = 1000, OverflowStrategy.fail)
      .toMat(Sink.asPublisher(true))(Keep.both).run()

  override def preStart(): Unit = {
    Ascii.createHeader(self)
  }

  def receive: Receive =  {
    case ArcAppStatus(_) =>
      sender() ! arcApp.get
    case ArcTaskUpdate(task) =>
      arcApp = updateTask(task)
    case TaskMasterStatus(_status) =>
      arcApp = arcApp.map(_.copy(status = Some(_status)))
    case req@ArcAppMetricRequest(id) if stateMasterConn.isDefined =>
      val stateMasterRef: ActorRef = stateMasterConn.get.ref
      (stateMasterRef ? req) pipeTo sender()
    case ArcAppMetricRequest(_) =>
      sender() ! ArcAppMetricFailure("AppMaster has no stateMaster tied to it. Cannot fetch metrics")
    case AppStream =>
      sender() ! publisher
    case PrintEvent(event) =>
      publisherRef ! event
  }

  protected def updateTask(task: ArcTask): Option[ArcApp] = {
    arcApp match {
      case Some(app) =>
        val updatedApp = app.copy(tasks = app.tasks.map(s => if (isSameTask(s.id, task.id)) task else s))
        val stillRunning = updatedApp
          .tasks
          .exists(_.result.isEmpty)

        Some(
          if (stillRunning) {
            if (updatedApp.status.get.equals(Identifiers.ARC_APP_DEPLOYING))
              updatedApp.copy(status = Some(Identifiers.ARC_APP_RUNNING))
            else
              updatedApp
          }
          else {
            updatedApp.copy(status = Some(Identifiers.ARC_TASK_KILLED))
          })
      case None =>
        None
    }
  }

  private def isSameTask(a: Option[Int], b: Option[Int]): Boolean = {
    (a,b) match {
      case (Some(x), Some(z)) => x == z
      case _ => false
    }
  }
}
