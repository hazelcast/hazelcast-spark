package com.hazelcast.spark.connector

import org.apache.spark.api.java.JavaPairRDD

import scala.reflect.ClassTag

class HazelcastJavaRDD[K, V](rdd: HazelcastRDD[K, V])(
  implicit override val kClassTag: ClassTag[K],
  implicit override val vClassTag: ClassTag[V])
  extends JavaPairRDD[K, V](rdd) {

}
