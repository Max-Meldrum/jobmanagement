package runtime.appmanager.core.actor

import akka.actor.{Actor, ActorLogging, Address, Props}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.MemberRemoved
import akka.cluster.metrics.StandardMetrics.{Cpu, HeapMemory}
import akka.cluster.metrics.{ClusterMetricsChanged, ClusterMetricsExtension, NodeMetrics}
import runtime.common.Identifiers

import scala.collection.mutable


private[runtime] object MetricAccumulator {
  def apply(): Props = Props(new MetricAccumulator())

  final case object ClusterMetrics
  final case object StateManagerMetrics

  sealed trait RuntimeMetric
  final case class CpuMetric(loadAverage: Double, processors: Int) extends RuntimeMetric
  final case class MemoryMetric(heapUsed: Double, heapCommited: Double, heapMax: Long) extends RuntimeMetric
  final case object UnknownMetric extends RuntimeMetric
  final case class ExhaustiveMetric(address: String, cpu: CpuMetric, mem: MemoryMetric) extends RuntimeMetric
}

/** Actor that collects host level metrics
  *
  * Currently gathers Cpu and HeapMemory objects from akka.cluster.metrics.
  * Gathers metrics only from TaskManagers and StateManagers
  */
private[runtime] class MetricAccumulator extends Actor with ActorLogging{
  import MetricAccumulator._

  private val metrics = ClusterMetricsExtension(context.system)
  private var stateManagerMetrics = mutable.HashMap[Address, ExhaustiveMetric]()


  override def preStart(): Unit = {
    metrics.subscribe(self)
    Cluster(context.system).
      subscribe(self, classOf[MemberRemoved])
  }

  override def postStop(): Unit = {
    metrics.unsubscribe(self)
    Cluster(context.system).unsubscribe(self)
  }


  def receive = {
    case ClusterMetricsChanged(nodeMetrics) =>
      nodeMetrics.foreach(handleMetrics)
    case StateManagerMetrics =>
      sender() ! stateManagerMetrics.values.toSeq
    case ClusterMetrics =>
    case MemberRemoved(m, _) if m.hasRole(Identifiers.STATE_MANAGER) =>
      stateManagerMetrics.remove(m.address)
  }

  /** Filter out where metrics are coming from and update
    * corresponding metric
    */
  private def handleMetrics(nodeMetrics: NodeMetrics): Unit = {
    val roles = Cluster(context.system)
      .state
      .members
      .filter(m => m.address == nodeMetrics.address)
      .map(_.roles)
      .flatten

    if (roles.contains(Identifiers.STATE_MANAGER))
      smMetricUpdate(nodeMetrics)
  }


  private def smMetricUpdate(nodeMetrics: NodeMetrics): Unit = {
    exhaustiveMetric(nodeMetrics)
      .map(m => stateManagerMetrics.put(nodeMetrics.address, m))
  }

  private def exhaustiveMetric(nodeMetrics: NodeMetrics): Option[ExhaustiveMetric] = {
    val cpu = cpuBuild(nodeMetrics)
    val mem = memBuild(nodeMetrics)
    val addr = nodeMetrics.address.toString
    if (cpu.isDefined && mem.isDefined)
      Some(ExhaustiveMetric(addr, cpu.get, mem.get))
    else
      None
  }

  private def cpuBuild(nodeMetrics: NodeMetrics): Option[CpuMetric] = nodeMetrics match {
    case Cpu(address, timestamp, Some(systemLoadAverage), cpuCombined, cpuStolen, processors) =>
      Some(CpuMetric(systemLoadAverage, processors))
    case _ =>
      None
  }

  private def memBuild(nodeMetrics: NodeMetrics): Option[MemoryMetric] = nodeMetrics match {
    case HeapMemory(address, timestamp, used, committed, max) =>
      Some(MemoryMetric(used.doubleValue / 1024 /1024, committed, max.getOrElse(-1)))
    case _ =>
      None
  }
}
