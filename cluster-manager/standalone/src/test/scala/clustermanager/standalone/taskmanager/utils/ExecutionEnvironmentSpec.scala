package clustermanager.standalone.taskmanager.utils

import java.nio.file.{Files, Paths}

import clustermanager.common.executor.ExecutionEnvironment
import org.scalatest.FlatSpec

import scala.util.{Failure, Success}

class ExecutionEnvironmentSpec extends FlatSpec {
  val jobId = "testjobid"
  val env = new ExecutionEnvironment(jobId)

  "Execution Environment" should "be created" in {
    env.create() match {
      case Success(_) =>
        assert(Files.exists(Paths.get(env.getJobPath)))
      case Failure(_) =>
        assert(fail)
    }
  }

  "Binary" should "be made executable" in {
    val binary = Files.write(Paths.get(env.getJobPath + "/" + jobId), "testdata".getBytes())
    env.writeBinaryToFile(jobId, Files.readAllBytes(binary))
    assert(Files.isExecutable(binary))
  }

  "Execution Environment" should "be cleaned" in {
    env.clean()
    assert(!Files.exists(Paths.get(env.getJobPath)))
  }

}
