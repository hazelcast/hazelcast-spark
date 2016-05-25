package com.hazelcast.spark

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

package object connector {

  implicit def toSparkContextFunctions(sc: SparkContext): HazelcastSparkContextFunctions =
    new HazelcastSparkContextFunctions(sc)

  implicit def toHazelcastRDDFunctions[K: ClassTag, V: ClassTag]
  (self: RDD[(K, V)]): HazelcastRDDFunctions[K, V] = new HazelcastRDDFunctions(self)


}