package runtime.tests.yarn



class DeploySpecMultiJvmNode1 extends DeploySpec
class DeploySpecMultiJvmNode2 extends DeploySpec


class DeploySpec extends RuntimeSpec {


  "AppManager" must {

    "wait node to join barrier" in {
      enterBarrier("startup")
    }

    "setup" in {

      runOn(appmanager) {
        enterBarrier("ready")
      }

      runOn(statemanager) {
        enterBarrier("ready")
      }
    }

    enterBarrier("finished")
  }
}
