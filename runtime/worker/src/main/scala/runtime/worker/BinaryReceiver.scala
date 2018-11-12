package runtime.worker

import akka.actor.{Actor, ActorLogging, Props}
import akka.io.Tcp.{PeerClosed, Received}
import akka.util.ByteString
import runtime.protobuf.messages.BinaryTransferred


private[worker] object BinaryReceiver {
  def apply(): Props =
    Props(new BinaryReceiver())
}

/** Actor that receives a binary and writes it to file
  *
  * BinaryReceiver is registered by Akka IO TCP to
  * handle a client connection. When the transfer is complete
  * the BinaryReceiver will receive a BinaryTransferred event
  * and write the binary to file in the specified Execution
  * Environment
  */
private[worker] class BinaryReceiver extends Actor with ActorLogging {
  private var buffer = ByteString.empty

  def receive = {
    case Received(data) =>
      buffer = buffer ++ data
    case PeerClosed =>
      context stop self
    case BinaryTransferred(id) =>
      /*
    case TaskUploaded(name) =>
      if (env.writeBinaryToFile(name, buffer.toArray)) {
        sender() ! TaskReady(name)
      } else {
        sender() ! TaskWriteFailure(name)
      }
      context stop self
      */
  }
}
