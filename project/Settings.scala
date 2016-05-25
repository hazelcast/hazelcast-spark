import sbt._
import Keys._
import sbtassembly.AssemblyKeys._
import sbtassembly._

object Settings {
  val buildName = "hazelcast-spark"
  val buildOrganization = "com.hazelcast"
  val buildVersion = "0.1-SNAPSHOT"
  val buildScalaVersion = "2.10.5"

  val buildSettings = Defaults.coreDefaultSettings ++ Seq(
    name := buildName,
    organization := buildOrganization,
    version := buildVersion,
    scalaVersion := buildScalaVersion,
    shellPrompt := ShellPrompt.buildShellPrompt,
    resolvers += Resolver.mavenLocal,
    parallelExecution in Test := false,
    test in assembly := {}
  )

}
