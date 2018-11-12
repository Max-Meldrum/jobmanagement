package runtime.worker

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import kamon.sigar.SigarProvisioner
import runtime.common.Identifiers
import runtime.protobuf.messages.ActorRefProto

private[worker] object WorkerApp extends App with LazyLogging {
  logger.info("Worker starting up")

  if (args.length >= 4) {
    val appId = args(0)
    val resourceBroker = args(2)
    val statemaster = args(3)
    val statemasterKompactAddr = args(4)

    // Makes sure it is loaded.
    loadSigar()

    val localhostname = java.net.InetAddress
      .getLocalHost
      .getHostAddress

    // Set up an ActorSystem that uses Remoting
    implicit val system = ActorSystem(Identifiers.CLUSTER, ConfigFactory.parseString(
      s"""
         | akka.actor.provider = remote
         | akka.actor.remote.enabled-transports = ["akka.remote.netty.tcp"]
         | akka.remote.netty.tcp.hostname = $localhostname
         | akka.remote.netty.tcp.port = 0
         | akka.actor.serializers.proto = "runtime.protobuf.ProtobufSerializer"
         | akka.actor.serializers.java = "akka.serialization.JavaSerializer"
         | akka.actor.serialization-bindings {"scalapb.GeneratedMessage" = proto}
    """.stripMargin))

    val resourceBrokerProto = ActorRefProto(resourceBroker)
    val stateMasterProto = ActorRefProto(statemaster)

    import runtime.protobuf.ProtoConversions.ActorRef._
    //system.actorOf(Worker(appId, containerId, resourceBrokerProto, stateMasterProto, statemasterKompactAddr), "executor")
    system.actorOf(Worker(), "worker")

    system.whenTerminated
  } else {
    println(System.getProperty("user.dir"))
    logger.error("Args are 1. App ID, 2. containerId, 3. ResourceBroker Ref, 4. stateMaster Ref, 5. StateMasterKompactProxy")
  }


  private def loadSigar(): Unit = {
    try {
      if (!SigarProvisioner.isNativeLoaded)
        SigarProvisioner.provision()
    } catch {
      case err: Exception =>
        logger.error("Could not initialize Sigar...")
    }
  }
}
