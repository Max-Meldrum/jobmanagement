package runtime.appmanager.yarn.client

import org.apache.hadoop.fs.Path
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.Records
import runtime.appmanager.yarn.util.YarnAppmanagerConfig

private[yarn] object YarnExecutor extends YarnAppmanagerConfig {
  import collection.JavaConverters._

  /** Creates a ContainerLaunchContext for our TaskExecutor
    *
    * @param brokerRef ActorRef in String
    * @param stateMasterRef ActorRef in String
    * @param stateMasterKompactProxy KompactProxyAddr in String
    * @param appId App id of ArcApp
    * @param containerId id of the container
    * @return ContainerLaunchContext
    */
  def context(brokerRef: String,
              stateMasterRef: String,
              stateMasterKompactProxy: String,
              appId:String,
              containerId: Long
             ): ContainerLaunchContext = {

    val conf = new YarnConfiguration()
    val ctx = Records.newRecord(classOf[ContainerLaunchContext])

    //  Commands
    ctx.setCommands(List(
      "$JAVA_HOME/bin/java" +
        " -Xmx256M " +
        s" $executorClass"+
        " " + appId +
        " " + containerId.toString +
        " " + brokerRef +
        " " + stateMasterRef +
        " " + stateMasterKompactProxy +
        " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
        " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
    ).asJava)

    val jarPath = new Path(executorJarPath +"/"+ executorJar)

    // Resources
    val resources = Map (executorJar -> YarnUtils.setLocalResource(jarPath, conf))
    ctx.setLocalResources(resources.asJava)

    // Environment
    ctx.setEnvironment(YarnUtils.setEnv(conf).asJava)

    ctx
  }

}
