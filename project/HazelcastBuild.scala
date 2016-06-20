import sbt.Keys._
import sbt._

import scala.tools.nsc.Properties

object HazelcastBuild extends Build {

  import Dependencies._
  import Settings._
  import Versions._

  val commonDeps = Seq(
    hazelcastClient,
    spark,
    jcache,
    junit,
    junitInterface
  )
  lazy val hazelcastSpark = Project(
    buildName,
    file("."),
    settings = buildSettings ++ Seq(
      libraryDependencies ++= commonDeps,
      publishArtifact in Test := false,
      crossPaths := false,
      publishMavenStyle := true,
      publishTo := {
        val nexus = "https://oss.sonatype.org/"
        if (isSnapshot.value)
          Some("snapshots" at nexus + "content/repositories/snapshots")
        else
          Some("releases" at nexus + "service/local/staging/deploy/maven2")
      },
      credentials += Credentials(file(sys.env.getOrElse("deployCredentials", Properties.userHome + "/.ivy2/.credentials"))),
      ivyXML := <dependencies>
        <dependency org="com.hazelcast" name="hazelcast" rev={hazelcastVersion} conf="compile->default(compile);provided->default(compile);test->default(compile)">
          <artifact name="hazelcast" type="jar" ext="jar" conf="compile"/>
          <artifact name="hazelcast" type="jar" ext="jar" conf="test" e:classifier="tests"/>
        </dependency>
      </dependencies>
    )
  )
}