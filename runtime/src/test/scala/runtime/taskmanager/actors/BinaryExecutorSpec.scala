package runtime.taskmanager.actors

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import runtime.ActorSpec
import runtime.common.{WeldTask, WeldTaskCompleted}
import runtime.taskmanager.utils.TaskManagerConfig

import scala.concurrent.duration._

class BinaryExecutorSpec extends TestKit(ActorSystem("BinaryExecutorSpec"))
  with ImplicitSender with ActorSpec with TaskManagerConfig {

  // UNIX bias
  private final val program = "ls"

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }


  "A BinaryExecutor Actor" must {

    "Receive updated object" in {
      val jm = TestProbe()
      val be = system.actorOf(BinaryExecutor(program, WeldTask("", ""), jm.ref))
      jm.expectMsgType[WeldTaskCompleted]
    }

    "Terminate after execution" in {
      val jm = TestProbe()
      val be = system.actorOf(BinaryExecutor(program, WeldTask("", ""), jm.ref))
      val probe = TestProbe()
      probe watch be
      probe.expectTerminated(be, binaryExecutorHealthCheck.millis + binaryExecutorHealthCheck.millis)
    }
  }

}
