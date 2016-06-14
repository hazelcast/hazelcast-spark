package com.hazelcast.spark.connector

import com.hazelcast.spark.connector.conf.SerializableConf
import com.hazelcast.spark.connector.rdd.HazelcastRDD
import com.hazelcast.spark.connector.util.CleanupUtil.addCleanupListener
import org.apache.spark.SparkContext

class HazelcastSparkContextFunctions(@transient val sc: SparkContext) extends Serializable {

  def fromHazelcastCache[K, V](cacheName: String): HazelcastRDD[K, V] = {
    addCleanupListener(sc)
    new HazelcastRDD[K, V](sc, cacheName, true, new SerializableConf(sc))
  }

  def fromHazelcastMap[K, V](mapName: String): HazelcastRDD[K, V] = {
    addCleanupListener(sc)
    new HazelcastRDD[K, V](sc, mapName, false, new SerializableConf(sc))
  }

}
