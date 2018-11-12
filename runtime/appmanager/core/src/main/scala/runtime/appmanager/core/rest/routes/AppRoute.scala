package runtime.appmanager.core.rest.routes


import akka.NotUsed
import akka.actor.ActorRef
import akka.http.scaladsl.model.ContentTypes._
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.server.Directives._
import runtime.appmanager.core.actor.AppManager._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.pattern._
import akka.stream.scaladsl.Source
import akka.util.{ByteString, Timeout}
import org.reactivestreams.Publisher
import runtime.appmanager.core.rest.JsonConverter
import runtime.protobuf.messages.{ArcApp, ArcAppMetricRequest, ArcAppMetricResponse, ArcTask}
import runtime.common._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

private[appmanager] class AppRoute(appManager: ActorRef)(implicit val ec: ExecutionContext) extends JsonConverter {
  implicit val timeout = Timeout(2.seconds)

  val route: Route =
    pathPrefix("apps") {
      path("deploy") {
        deploy
      }~
        path("deploy_streamed") {
          deployStream
        }~
        path("metrics" / Segment) { appId: String =>
          complete(fetchAppMetrics(appId))
        }~
        path("kill" / Segment) { appId: String =>
          complete("killing app...")
        }~
        path("status" / Segment) { appId: String =>
          complete(appStatus(appId))
        }~
        path("list") {
          complete(listApps())
        }~
        path("listfull") {
          complete("list all apps but with details")
        }
      }


  private def fetchAppMetrics(id: String): Future[ArcAppMetricResponse] =
    (appManager ? ArcAppMetricRequest(id)).mapTo[ArcAppMetricResponse]

  private def killApp(id: String): Future[String] =
    (appManager ? KillArcAppRequest(id)).mapTo[String]

  private def appStatus(id: String): Future[ArcApp] =
    (appManager ? ArcAppStatus(id)).mapTo[ArcApp]

  private def listApps(): Future[Seq[ArcApp]] =
    (appManager ? ListApps).mapTo[Seq[ArcApp]]

  private def listAppsWithDetails(): Future[Any] =
    (appManager ? ListAppsWithDetails).mapTo[String]


  /** Deploy route which returns the App ID
    * and processes it in the background
    */
  private def deploy: Route = {
    entity(as[ArcDeployRequest]) { req =>
      val indexedTasks = indexTasks(req.tasks)

      val arcApp = ArcApp(IdGenerator.app(), indexedTasks, req.priority, true,
        status = Some(Identifiers.ARC_APP_DEPLOYING))
      val appRequest = ArcAppRequest(arcApp)
      val f = (appManager ? appRequest).mapTo[String]
      complete(f)
    }
  }

  /** Deploy route which streams a live view of
    * the deployment process back to the client
    */
  private def deployStream: Route = {
    entity(as[ArcDeployRequest]) { req =>
      val indexedTasks = indexTasks(req.tasks)

      val arcApp = ArcApp(IdGenerator.app(), indexedTasks, req.priority, true,
        status = Some(Identifiers.ARC_APP_DEPLOYING))
      val publisherFut = (appManager ? ArcAppRequest(arcApp, liveFeed = true)).mapTo[Publisher[ByteString]]

      val resp = publisherFut map {
        case pub: Publisher[ByteString] =>
          val source = Source.fromPublisher(pub)
          HttpEntity(`text/plain(UTF-8)`, source)
        case _ =>
          HttpEntity(`text/plain(UTF-8)`, "Something went wrong while fetching publisher")
      }
      complete(resp)
    }
  }

  private def indexTasks(tasks: Seq[ArcTask]): Seq[ArcTask] =
    tasks.zipWithIndex.map(m => m._1.copy(id = Some(m._2 + 1)))

}
