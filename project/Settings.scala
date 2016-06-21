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
    test in assembly := {},
    pomExtra := <licenses>
      <license>
        <name>The Apache Software License, Version 2.0</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
        <distribution>repo</distribution>
      </license>
    </licenses>
      <scm>
        <connection>scm:git:git://github.com/hazelcast/hazelcast-spark.git</connection>
        <developerConnection>scm:git:git@github.com:hazelcast/hazelcast-spark.git</developerConnection>
        <url>https://github.com/hazelcast/hazelcast-spark/</url>
      </scm>
      <developers>
        <developer>
          <id>eminn</id>
          <name>emin demirci</name>
          <email>emin@hazelcast.com</email>
        </developer>
      </developers>
      <issueManagement>
        <system>Github</system>
        <url>https://github.com/hazelcast/hazelcast-spark/issues</url>
      </issueManagement>
      <organization>
        <name>Hazelcast, Inc.</name>
        <url>http://www.hazelcast.com/</url>
      </organization>
  )

}
