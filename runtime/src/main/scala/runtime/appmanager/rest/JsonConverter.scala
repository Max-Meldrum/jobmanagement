package runtime.appmanager.rest

import runtime.appmanager.actors.AppManager.ArcDeployRequest
import runtime.appmanager.actors.MetricAccumulator._
import runtime.common.messages._
import spray.json.DefaultJsonProtocol
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import runtime.appmanager.rest.routes.ClusterRoute.NamedMetric



/**
  * JSON Marshaller/Unmarshaller
  * Should probably look into changing this to Circe later on..
  */
trait JsonConverter extends SprayJsonSupport with DefaultJsonProtocol {
  import spray.json._

  implicit val arcTaskFormat = jsonFormat6(ArcTask.apply)
  implicit val arcDeployRequest = jsonFormat1(ArcDeployRequest.apply)
  implicit val actorRefProtoFormat = jsonFormat1(ActorRefProto.apply)
  implicit val arcProfileFormat = jsonFormat2(ArcProfile.apply)
  implicit val arcJobFormat = jsonFormat5(ArcJob.apply)



  // Runtime Metrics
  implicit val cpuMetricFormat = jsonFormat2(CpuMetric.apply)
  implicit val memMetricFormat = jsonFormat3(MemoryMetric.apply)
  implicit val exhaustiveMetricFormat = jsonFormat3(ExhaustiveMetric.apply)
  implicit val namedMetricFormat = jsonFormat2(NamedMetric.apply)
  implicit val arcMetricFormat1 = lift(new JsonWriter[ArcMetric] {
    override def write(obj: ArcMetric): JsValue = obj match {
      case cpu: CpuMetric => cpu.toJson
      case mem: MemoryMetric => mem.toJson
      case ex: ExhaustiveMetric => ex.toJson
      case UnknownMetric => throw new RuntimeException("Unknown JSON Format")
    }
  })


  // ExecutorMetric
  implicit val processStateFormat = jsonFormat3(ProcessState.apply)
  implicit val cpuFormat = jsonFormat6(Cpu.apply)
  implicit val ioFormat = jsonFormat3(IO.apply)
  implicit val memFormat = jsonFormat5(Mem.apply)
  implicit val executorFormat = jsonFormat2(Executor.apply)
  implicit val executorMetricFormat = jsonFormat6(ExecutorMetric.apply)

  // ArcJobMetric
  implicit val arcTaskMetricFormat = jsonFormat2(ArcTaskMetric.apply)
  implicit val arcMetricReportFormat = jsonFormat2(ArcJobMetricReport.apply)
  implicit val arcMetricFailureFormat = jsonFormat1(ArcJobMetricFailure.apply)
  implicit object ArcJobMetrcResp extends RootJsonFormat[ArcJobMetricResponse] {
    def write(obj: ArcJobMetricResponse): JsValue = obj match {
      case report: ArcJobMetricReport => report.toJson
      case fail: ArcJobMetricFailure => fail.toJson
      case _ => throw new RuntimeException("Unknown JSON Format")
    }

    def read(json: JsValue): ArcJobMetricResponse = ???
  }
}
