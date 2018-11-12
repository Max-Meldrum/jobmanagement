package runtime.appmanager.core.rest.routes

import akka.actor.ActorRef
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import runtime.appmanager.core.rest.JsonConverter
import akka.pattern._
import akka.util.Timeout
import runtime.common.Identifiers

import scala.concurrent.{ExecutionContext, Future}

object ClusterRoute {
  import runtime.appmanager.core.actor.MetricAccumulator._
  import scala.concurrent.duration._

  final case class NamedMetric(name: String, metrics: ExhaustiveMetric)


  /** api/$version/cluster/metrics
    * GET -> metrics
    * @param am ActorRef to AppManager
    */
  final case class ClusterOverviewRoute(am: ActorRef)(implicit ec: ExecutionContext)
    extends JsonConverter {
    implicit val timeout = Timeout(2.seconds)

    def route()(implicit ec:ExecutionContext): Route = {
      path("metrics") {
        get {
          onSuccess(collect()) {s =>
            complete(s)
          }
        }
      }
    }

    private def collect(): Future[Seq[NamedMetric]] = {
      for {
        sm <- (am ? StateManagerMetrics).mapTo[Seq[ExhaustiveMetric]]
      } yield sm.map(s => NamedMetric(Identifiers.STATE_MANAGER, s))
    }
  }

  object ClusterOverviewRoute {
    def apply(am: ActorRef)(implicit ec: ExecutionContext): Route =
      new ClusterOverviewRoute(am).route()
  }

  /** api/$version/cluster/statemanager/
    * GET -> metrics
    * @param am ActorRef to AppManager
    */
  final case class StateManagerRoute(am: ActorRef) extends JsonConverter {
    implicit val timeout = Timeout(2.seconds)

    def route(): Route = {
      pathPrefix("statemanager") {
        pathPrefix("metrics") {
          get {
            onSuccess((am ? StateManagerMetrics).mapTo[Seq[ExhaustiveMetric]]) { res =>
              complete(res)
            }
          }
        }
      }
    }
  }

  object StateManagerRoute {
    def apply(am: ActorRef): Route = new StateManagerRoute(am).route()
  }
}

class ClusterRoute(appManager: ActorRef)(implicit val ec: ExecutionContext) {
  import ClusterRoute._

  val route: Route =
    pathPrefix("cluster") {
      ClusterOverviewRoute(appManager)~
      StateManagerRoute(appManager)
    }
}
