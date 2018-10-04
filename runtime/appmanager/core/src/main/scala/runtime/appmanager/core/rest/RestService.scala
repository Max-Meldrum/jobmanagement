package runtime.appmanager.core.rest

import akka.actor.ActorRef

import scala.concurrent.ExecutionContext
import akka.http.scaladsl.server.Directives._
import runtime.appmanager.core.rest.routes.{AppRoute, ClusterRoute}
import runtime.appmanager.core.util.AppManagerConfig

private[appmanager] class RestService(appManager: ActorRef)(implicit val ec: ExecutionContext)
  extends AppManagerConfig {

  private val appRoute = new AppRoute(appManager)
  private val clusterRoute = new ClusterRoute(appManager)


  val route =
    pathPrefix("api") {
      pathPrefix(restVersion) {
        clusterRoute.route~
        appRoute.route
      }
    }

}
