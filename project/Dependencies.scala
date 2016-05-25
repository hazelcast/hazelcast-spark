import sbt._
import Versions._

object Dependencies {
  val hazelcast = "com.hazelcast" % "hazelcast" % hazelcastVersion
  val hazelcastClient = "com.hazelcast" % "hazelcast-client" % hazelcastVersion
  val spark = "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
  val jcache = "javax.cache" % "cache-api" % jcacheVersion

  // Test
  val junit = "junit" % "junit" % junitVersion % "test"
  val hazelcastTest = "com.hazelcast" % "hazelcast" % hazelcastVersion % "provided"  classifier "tests"
  val junitInterface = "com.novocode" % "junit-interface" % "0.8" % "test->default"

}
