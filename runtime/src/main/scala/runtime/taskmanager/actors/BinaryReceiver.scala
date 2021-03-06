package runtime.taskmanager.actors

import akka.actor.{Actor, ActorLogging, Props}
import akka.io.Tcp.{PeerClosed, Received}
import akka.util.ByteString
import runtime.taskmanager.utils.ExecutionEnvironment


object BinaryReceiver {
  def apply(id: String, env: ExecutionEnvironment): Props =
    Props(new BinaryReceiver(id, env))
}

/** Actor that receives a Rust binary and writes it to file
  *
  * BinaryReceiver is registered by Akka IO TCP to
  * handle a client connection. When the transfer is complete
  * the BinaryReceiver will receive a BinaryUploaded event
  * and write the binary to file in the specified Execution
  * Environment
  */
class BinaryReceiver(id: String, env: ExecutionEnvironment)
  extends Actor with ActorLogging {

  import BinaryManager._

  private var buffer = ByteString.empty

  def receive = {
    case Received(data) =>
      buffer = buffer ++ data
    case PeerClosed =>
      log.info("Peer closed for: " + id)
      context stop self
    case BinaryUploaded =>
      if (env.writeBinaryToFile(id, buffer.toArray)) {
        sender() ! BinaryReady(id)
      } else {
        sender() ! BinaryWriteFailure
      }
      context stop self
  }
}
