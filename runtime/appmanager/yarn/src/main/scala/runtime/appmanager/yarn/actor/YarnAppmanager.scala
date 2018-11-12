package runtime.appmanager.yarn.actor

import akka.actor.{ActorRef, Props}
import runtime.appmanager.core.actor.AppManager
import runtime.common.Identifiers
import runtime.protobuf.messages.ArcApp


private[runtime] object YarnAppmanager {
  def apply(): Props = Props(new YarnAppmanager)
}

private[runtime] class YarnAppmanager extends AppManager {
  import runtime.appmanager.core.actor.AppManager._
  import akka.pattern._

  override def receive = super.receive orElse {
    case ArcAppRequest(arcApp, true) =>
      val appMaster = createAppMaster(arcApp)
      (appMaster ? AppStream) pipeTo sender()
    case ArcAppRequest(arcApp, false) =>
      val appMaster = createAppMaster(arcApp)
      val response = "Processing App: " + arcApp.id + "\n"
      sender() ! response
  }

  private def createAppMaster(app: ArcApp): ActorRef = {
    val appMaster = context.actorOf(YarnAppMaster(app), Identifiers.APP_MASTER+appMasterId)
    appMasterId +=1
    appMasters = appMasters :+ appMaster
    appMap.put(app.id, appMaster)

    // Enable DeathWatch
    context watch appMaster

    // Create a state master that is linked with the AppMaster and TaskMaster
    getStateMaster(appMaster, app) recover {case _ => StateMasterError} pipeTo appMaster

    appMaster
  }

}
