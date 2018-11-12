
import sbt._
import Keys._

object Dependencies {
  //TODO check versions
  val scalatestVersion = "3.0.1"
  val loggingVersion = "3.7.2"
  val logbackVersion = "1.2.3"
  val typeConfigVersion = "1.3.1"
  val akkaVersion = "2.5.12"
  val akkaHttpVersion = "10.1.3"
  val akkaStreamsVersion = akkaVersion
  val yarnVersion = "3.1.0"
  val nettyVersion = "4.1.28.Final"


  val testDependencies: Seq[ModuleID] = Seq(
    "org.scalatest" %% "scalatest" % scalatestVersion % "test"
  )

  val logDependencies: Seq[ModuleID] = Seq(
    "com.typesafe.scala-logging" %% "scala-logging" % loggingVersion
    //"ch.qos.logback" % "logback-classic" % logbackVersion
  )

  val logbackDependencies: Seq[ModuleID] = Seq(
    "ch.qos.logback" % "logback-classic" % logbackVersion
  )

  val confDependencies: Seq[ModuleID] = Seq(
    "com.typesafe" % "config" % typeConfigVersion
  )

  val nettyDependencies: Seq[ModuleID] = Seq(
    "io.netty" % "netty-all" % nettyVersion
  )

  val simpleAkkaDependencies: Seq[ModuleID] = Seq(
    "com.typesafe.akka" %% "akka-actor" % akkaVersion,
    "com.typesafe.akka" %% "akka-testkit" % akkaVersion
  )

  val akkaDependencies: Seq[ModuleID] = Seq(
    "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
    "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
    "com.typesafe.akka" %% "akka-cluster-metrics" % akkaVersion
  )

  val akkaRemoteDependencies: Seq[ModuleID] = Seq(
    "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
    "com.typesafe.akka" %% "akka-remote" % akkaVersion
  )

  val akkaMultiNodeDependencies: Seq[ModuleID] = Seq(
    "com.typesafe.akka" %% "akka-multi-node-testkit" % akkaVersion
  )


  import scalapb.compiler.Version.scalapbVersion
  val protobufDependencies: Seq[ModuleID] = Seq(
    "com.thesamet.scalapb" %% "scalapb-runtime" % scalapbVersion % "protobuf"
  )

  val yarnDependencies: Seq[ModuleID] = Seq(
    "org.apache.hadoop" % "hadoop-yarn-client" % yarnVersion,
    "org.apache.hadoop" % "hadoop-yarn-api" % yarnVersion,
    "org.apache.hadoop" % "hadoop-client" % yarnVersion,
    "org.apache.hadoop" % "hadoop-hdfs"  % yarnVersion % Test classifier "tests"
  )

  val lightHdfsDependencies: Seq[ModuleID] = Seq(
    "org.apache.hadoop" % "hadoop-client" % yarnVersion,
    "org.apache.hadoop" % "hadoop-hdfs"  % yarnVersion % Test classifier "tests"
  )


  val akkaHttpDependencies: Seq[ModuleID] = Seq(
    "com.typesafe.akka" %% "akka-http"   % akkaHttpVersion,
    "com.typesafe.akka" %% "akka-stream" % akkaStreamsVersion,
    "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion
  )

  val sigarDependencies: Seq[ModuleID] = Seq(
    "io.kamon" % "sigar-loader" % "1.6.6-rev002"
  )

  // Common libs that are used together
  val basic : Seq[ModuleID] =
    logDependencies ++ confDependencies ++ testDependencies

  // Actual Dependencies
  val statemanagerDeps: Seq[ModuleID] = basic ++ akkaDependencies ++ sigarDependencies
  val appmanagerCoreDeps: Seq[ModuleID] = basic ++ akkaDependencies ++ sigarDependencies ++ akkaHttpDependencies
  val yarnAppmanagerDeps: Seq[ModuleID] = appmanagerCoreDeps ++ yarnDependencies
  val protobufDeps: Seq[ModuleID] = testDependencies ++ protobufDependencies ++ akkaDependencies
  val kompactExtDeps: Seq[ModuleID] = basic ++ protobufDependencies ++ nettyDependencies ++ simpleAkkaDependencies ++ logbackDependencies
  val runtimeCommonDeps: Seq[ModuleID] = testDependencies ++ logDependencies ++ simpleAkkaDependencies
  val runtimeTestsDeps: Seq[ModuleID] = basic ++ akkaMultiNodeDependencies ++ akkaDependencies
  val workerDeps: Seq[ModuleID] = basic ++ akkaRemoteDependencies ++ sigarDependencies
  val yarnExecutorDeps: Seq[ModuleID] = basic ++ akkaRemoteDependencies ++ sigarDependencies ++ lightHdfsDependencies
  // to be removed.
  val executorCommonDeps: Seq[ModuleID] = basic ++ sigarDependencies


  // Helpers
  val statemanager = libraryDependencies ++= statemanagerDeps
  val appmanagerCore = libraryDependencies ++= appmanagerCoreDeps
  val appmanagerYarn = libraryDependencies ++= yarnAppmanagerDeps
  val protobuf = libraryDependencies ++= protobufDeps
  val kompactExtension = libraryDependencies ++= kompactExtDeps
  val runtimeCommon = libraryDependencies ++= runtimeCommonDeps
  val runtimeTests = libraryDependencies ++= runtimeTestsDeps
  val yarnExecutor = libraryDependencies ++= yarnExecutorDeps
  val executorCommon = libraryDependencies ++= executorCommonDeps
  val worker = libraryDependencies ++= workerDeps
}