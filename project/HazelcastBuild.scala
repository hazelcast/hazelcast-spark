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
      publishTo := Some("Cloudbees Snapshot Repository" at "https://repository-hazelcast-l337.forge.cloudbees.com/snapshot/"),
      credentials += Credentials(file(sys.env.getOrElse("deployCredentials", Properties.userHome + ".ivy2/.credentials"))),
      ivyXML := <dependencies>
        <dependency org="com.hazelcast" name="hazelcast" rev={hazelcastVersion} conf="compile->default(compile);provided->default(compile);test->default(compile)">
          <artifact name="hazelcast" type="jar" ext="jar" conf="compile"/>
          <artifact name="hazelcast" type="jar" ext="jar" conf="test" e:classifier="tests"/>
        </dependency>
      </dependencies>
    )
  )
}