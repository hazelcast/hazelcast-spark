import sbt.Keys._
import sbt._

import scala.xml.{Node => XmlNode, NodeSeq => XmlNodeSeq}


object HazelcastBuild extends Build {

  import Dependencies._
  import Settings._
  import Versions._

  val commonDeps = Seq(
    hazelcastClient,
    spark,
    jcache,
    junit
  )
  lazy val hazelcastSpark = Project(
    buildName,
    file("."),
    settings = buildSettings ++ Seq(
      libraryDependencies ++= commonDeps,
      publishArtifact in Test := false,
      publishMavenStyle := true,
      ivyXML := <dependencies>
        <dependency org="com.hazelcast" name="hazelcast" rev={hazelcastVersion} conf="compile->default(compile);provided->default(compile)">
          <artifact name="hazelcast" type="jar" ext="jar" conf="compile"/>
          <artifact name="hazelcast" type="jar" ext="jar" conf="test" e:classifier="tests"/>
        </dependency>
      </dependencies>
    )
  )
}