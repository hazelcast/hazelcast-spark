import sbt.Keys._
import sbt._
import sbtassembly.AssemblyKeys._

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
    resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
    resolvers += Resolver.mavenLocal,
    parallelExecution in Test := false,
    test in assembly := {}
  )

}
