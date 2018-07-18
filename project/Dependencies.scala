
import sbt._
import Keys._

object Dependencies {
  //TODO check versions
  val scalatestVersion = "3.0.1"
  val loggingVersion = "3.5.0"
  val logbackVersion = "1.2.3"
  val typeConfigVersion = "1.3.1"
  val akkaVersion = "2.5.12"
  val akkaHttpVersion = "10.1.3"
  val akkaStreamsVersion = akkaVersion
  val yarnVersion = "3.1.0"


  val testDependencies: Seq[ModuleID] = Seq(
    "org.scalatest" %% "scalatest" % scalatestVersion % "test"
  )

  val logDependencies: Seq[ModuleID] = Seq(
    "com.typesafe.scala-logging" %% "scala-logging" % loggingVersion,
    "ch.qos.logback" % "logback-classic" % logbackVersion
  )
  val confDependencies: Seq[ModuleID] = Seq(
    "com.typesafe" % "config" % typeConfigVersion
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
    "org.apache.hadoop" % "hadoop-client" % yarnVersion
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
  val appmanagerDeps: Seq[ModuleID] = basic ++ akkaDependencies ++ sigarDependencies ++ akkaHttpDependencies
  val protobufDeps: Seq[ModuleID] = testDependencies ++ protobufDependencies ++ akkaDependencies
  val runtimeCommonDeps: Seq[ModuleID] = testDependencies ++ logDependencies ++ simpleAkkaDependencies
  val standaloneManagerDeps: Seq[ModuleID] = basic ++ akkaDependencies ++ sigarDependencies
  val taskmasterDeps: Seq[ModuleID] = basic ++ akkaDependencies ++ sigarDependencies
  val yarnManagerDeps: Seq[ModuleID] = basic ++ yarnDependencies
  val runtimeTestsDeps: Seq[ModuleID] = basic ++ akkaMultiNodeDependencies ++ akkaDependencies

  // Helpers
  val statemanager = libraryDependencies ++= statemanagerDeps
  val appmanager = libraryDependencies ++= appmanagerDeps
  val taskmaster = libraryDependencies ++= taskmasterDeps
  val protobuf = libraryDependencies ++= protobufDeps
  val runtimeCommon = libraryDependencies ++= runtimeCommonDeps
  val runtimeTests = libraryDependencies ++= runtimeTestsDeps
  val standalone = libraryDependencies ++= standaloneManagerDeps
  val yarnManager = libraryDependencies ++= yarnManagerDeps
}