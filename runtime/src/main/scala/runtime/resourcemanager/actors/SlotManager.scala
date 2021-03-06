package runtime.resourcemanager.actors

import akka.actor.{Actor, ActorLogging, Address, Props}
import runtime.common._

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

object SlotManager {
  def apply(): Props = Props(new SlotManager)
}

/**
  * SlotManager is responsible for handling the TaskSlots of all
  * registered TaskManagers.
  */
class SlotManager extends Actor with ActorLogging {
  import ClusterListener._
  import ResourceManager._
  import runtime.common.Types._

  var taskManagers = mutable.IndexedSeq.empty[TaskManagerAddr]
  val slots = mutable.HashMap[Address, Seq[TaskSlot]]()
  var roundNumber = 0


  def receive = {
    case TaskManagerRegistration(tm) if !taskManagers.contains(tm) =>
      taskManagers = taskManagers :+ tm
      val target = context.actorSelection(ActorPaths.taskManager(tm))
      // TODO: add retry logic in case worker is not reachable
      // in order to make sure that the TaskManager is initialized
      target ! TaskManagerInit
    case TaskManagerRemoved(tm) =>
      log.info("TaskManager Removed")
      cleanTaskManager(tm)
    case UnreachableTaskManager(tm) =>
      log.info("TaskManager Unreachable")
      cleanTaskManager(tm)
    case SlotUpdate(s) =>
      slots.put(sender().path.address, s)
      log.info(slots.toString())
    case req@SlotRequest(job) =>
      //TODO: Clean and improve
      handleSlotRequest(req) match {
        case NoSlotsAvailable =>
          log.info("No Slots Available")
          sender() ! NoSlotsAvailable
        case NoTaskManagersAvailable =>
          log.info("No Task Managers Available")
          sender() ! NoTaskManagersAvailable
        case SlotAvailable(taskSlots, addr) =>
          log.info("Slots Available")
          val taskManager = context.actorSelection(ActorPaths.taskManager(addr))
          taskManager forward Allocate(job, taskSlots)
        case UnexpectedError =>
          // TODO: fix
      }
      roundNumber += 1
    case _ =>
  }

  private def cleanTaskManager(tm: TaskManagerAddr): Unit = {
    Try {
      taskManagers = taskManagers.filterNot(_ == tm)
      slots.remove(tm)
    } match {
      case Success(_) => // ignore
      case Failure(e) => log.error("Error while cleaning TaskManager")
    }
  }

  //TODO: Make more readable?
  private def handleSlotRequest(req: SlotRequest): SlotRequestResp = {
    if (roundNumber > taskManagers.size-1)
      roundNumber = 0

    if (taskManagers.isEmpty) {
      NoTaskManagersAvailable
    } else if (roundNumber <= taskManagers.size) {
      slots.get(taskManagers(roundNumber)) match {
        case Some(seq) =>
          val freeSlots = seq.filter(slot => slot.state == Free)
          if (freeSlots.isEmpty)
            NoSlotsAvailable
          else {
            val resources = freeSlots.foldLeft((ArcProfile(0.0, 0), Seq[TaskSlot]())) { (x, y) =>
              if (x._1.matches(req.job.profile))
                x
              else
                (x._1.copy(cpuCores = x._1.cpuCores + y.profile.cpuCores,
                  memoryInMB = x._1.memoryInMB + y.profile.memoryInMB), x._2 :+ y)
            }

            if (resources._1.matches(req.job.profile)) {
              SlotAvailable(resources._2, taskManagers(roundNumber))
            } else {
              NoSlotsAvailable
            }
          }
        case None =>
          NoSlotsAvailable
      }
    } else {
      NoSlotsAvailable
    }
  }
}
