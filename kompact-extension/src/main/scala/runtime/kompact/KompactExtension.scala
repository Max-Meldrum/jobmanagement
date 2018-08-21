package runtime.kompact

import akka.actor.{ActorRef, ActorSystem, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}

private[kompact] object KompactExtensionImpl {
  case class Register(actorRef: ActorRef)
  case class Unregister(actorRef: ActorRef)
}

class KompactExtensionImpl(system: ExtendedActorSystem) extends Extension {
  import KompactExtensionImpl._

  private val proxyActor = system.actorOf(ProxyActor(), "kompactproxy")

  /** Interested actors register to the
    * KompactProxy
    * @param actorRef ActorRef
    */
  def register(actorRef: ActorRef): Unit =
    proxyActor ! Register(actorRef)

  /** unregister is called when an Akka actor is shutdown.
    * @param actorRef ActorRef
    */
  def unregister(actorRef: ActorRef): Unit =
    proxyActor ! Unregister(actorRef)

  // Return socket info for the kompact extension
  def getInfo(): String = ???
}

/** Akka Extension that loads a Proxy Server in order to
  * establish communication between Akka Actors and Kompact Actors
  */
object KompactExtension extends ExtensionId[KompactExtensionImpl] with ExtensionIdProvider {
  override def lookup(): ExtensionId[_ <: Extension] = KompactExtension
  override def get(system: ActorSystem): KompactExtensionImpl = super.get(system)
  override def createExtension(system: ExtendedActorSystem): KompactExtensionImpl =
    new KompactExtensionImpl(system)
}

