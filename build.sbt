import com.typesafe.sbt.SbtMultiJvm.multiJvmSettings
import com.typesafe.sbt.SbtMultiJvm.MultiJvmKeys.MultiJvm

name := "jobmanagement." + "root"

scalacOptions ++= Seq(
  "-deprecation",
  "-encoding",
  "UTF-8",
  "-feature",
  "-unchecked"
)

lazy val asciiGraphs = RootProject(uri("git://github.com/Max-Meldrum/ascii-graphs.git"))
version in asciiGraphs := "0.0.7-SNAPSHOT"


lazy val generalSettings = Seq(
  // can be changed
  organization := "se.kth.cda",
  scalaVersion := "2.12.6"
)

lazy val runtimeSettings = generalSettings ++ Seq(
  fork in run := true,  // https://github.com/sbt/sbt/issues/3736#issuecomment-349993007
  cancelable in Global := true,
  version := "0.1",
  javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled"),
  fork in Test := true
)

lazy val runtimeMultiJvmSettings = multiJvmSettings ++ Seq(
  // For loading Sigar
  jvmOptions in MultiJvm += s"-Djava.library.path=${"target/native"}"
)


lazy val root = (project in file("."))
  .aggregate(statemanager, appmanagerCore, appmanagerYarn, runtimeProtobuf,
    runtimeCommon, runtimeTests, kompactExtension, worker,
    yarnExecutor, executorCommon)


lazy val statemanager = (project in file("runtime/statemanager"))
  .dependsOn(runtimeProtobuf, runtimeCommon, kompactExtension % "test->test; compile->compile")
  .settings(runtimeSettings: _*)
  .settings(Dependencies.statemanager)
  .settings(moduleName("runtime.statemanager"))
  .settings(Assembly.settings("runtime.statemanager.SmSystem", "statemanager.jar"))
  .settings(Sigar.loader())


lazy val appmanagerCore = (project in file("runtime/appmanager/core"))
  .dependsOn(runtimeProtobuf, runtimeCommon, asciiGraphs % "test->test; compile->compile")
  .settings(runtimeSettings: _*)
  .settings(Dependencies.appmanagerCore)
  .settings(moduleName("runtime.appmanager.core"))

lazy val appmanagerYarn = (project in file("runtime/appmanager/yarn"))
  .dependsOn(appmanagerCore % "test->test; compile->compile")
  .settings(runtimeSettings: _*)
  .settings(Dependencies.appmanagerYarn)
  .settings(moduleName("runtime.appmanager.yarn"))
  .settings(Assembly.settings("runtime.appmanager.yarn.YarnManager", "yarn_appmanager.jar"))
  .settings(Sigar.loader())

lazy val runtimeProtobuf = (project in file("runtime/protobuf"))
  .settings(runtimeSettings: _*)
  .settings(Dependencies.protobuf)
  .settings(moduleName("runtime.protobuf"))
  .settings(
    PB.targets in Compile := Seq(
      scalapb.gen() -> (sourceManaged in Compile).value
    )
  )

lazy val runtimeCommon = (project in file("runtime/common"))
  .settings(runtimeSettings: _*)
  .settings(Dependencies.runtimeCommon)
  .settings(moduleName("runtime.common"))

lazy val kompactExtension = (project in file("kompact-extension"))
  .settings(runtimeSettings: _*)
  .settings(Dependencies.kompactExtension)
  .settings(moduleName("runtime.kompact"))
  .settings(
    PB.targets in Compile := Seq(
      scalapb.gen() -> (sourceManaged in Compile).value
    )
  )


lazy val runtimeTests = (project in file("runtime/tests"))
  .dependsOn(
    runtimeProtobuf, runtimeCommon % "test->test; compile->compile",
    statemanager, appmanagerCore, appmanagerYarn % "test->test; compile->compile"
  )
  .settings(runtimeSettings: _*)
  .settings(Dependencies.runtimeTests)
  .settings(moduleName("runtime.tests"))
  .enablePlugins(MultiJvmPlugin)
  .configs(MultiJvm)
  .settings(Sigar.loader())
  .settings(
    parallelExecution in Test := false // do not run test cases in
  )

lazy val worker = (project in file("runtime/worker"))
  .dependsOn(runtimeProtobuf, runtimeCommon % "test->test; compile->compile")
  .settings(runtimeSettings: _*)
  .settings(Dependencies.worker)
  .settings(moduleName("runtime.worker"))
  .settings(Assembly.settings("runtime.worker.Worker", "worker.jar"))

lazy val executorCommon = (project in file("executor/common"))
  .dependsOn(runtimeProtobuf % "test->test; compile->compile")
  .settings(runtimeSettings: _*)
  .settings(Dependencies.executorCommon)
  .settings(moduleName("executor.common"))

lazy val yarnExecutor = (project in file("executor/yarn"))
  .dependsOn(runtimeProtobuf, runtimeCommon, executorCommon % "test->test; compile->compile")
  .settings(runtimeSettings: _*)
  .settings(Dependencies.yarnExecutor)
  .settings(moduleName("executor.yarn"))
  .settings(Assembly.settings("executor.yarn.ExecutorApp", "yarn-executor.jar"))


def moduleName(m: String): Def.SettingsDefinition = {
  val mn = "Module"
  packageOptions in (Compile, packageBin) += Package.ManifestAttributes(mn → m)
}
