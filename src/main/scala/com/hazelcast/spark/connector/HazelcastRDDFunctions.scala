package com.hazelcast.spark.connector

import com.hazelcast.client.cache.impl.ClientCacheProxy
import com.hazelcast.client.proxy.ClientMapProxy
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.spark.connector.ConnectionManager.getHazelcastConnection
import com.hazelcast.spark.connector.HazelcastHelper.{getClientCacheProxy, getClientMapProxy}
import com.hazelcast.spark.connector.Properties.{getServerAddress, getWriteBatchSize}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._

class HazelcastRDDFunctions[K, V](val rdd: RDD[(K, V)]) extends Serializable {

  def saveToHazelcastCache(cacheName: String): Unit = {
    val server: String = getServerAddress(rdd.sparkContext)
    val writeBatchSize: Int = getWriteBatchSize(rdd.sparkContext)
    val job = (ctx: TaskContext, iterator: Iterator[(K, V)]) => {
      new HazelcastWriteToCacheJob(writeBatchSize).runJob(ctx, iterator, cacheName, server)
    }
    rdd.sparkContext.runJob(rdd, job)
  }

  def saveToHazelcastMap(mapName: String): Unit = {
    val server: String = getServerAddress(rdd.sparkContext)
    val writeBatchSize: Int = getWriteBatchSize(rdd.sparkContext)
    val job = (ctx: TaskContext, iterator: Iterator[(K, V)]) => {
      new HazelcastWriteToMapJob(writeBatchSize).runJob(ctx, iterator, mapName, server)
    }
    rdd.sparkContext.runJob(rdd, job)
  }

  private class HazelcastWriteToCacheJob(val writeBatchSize: Int) extends Serializable {
    def runJob(ctx: TaskContext, iterator: Iterator[(K, V)], cacheName: String, server: String): Unit = {
      val client: HazelcastInstance = getHazelcastConnection(server);
      val cache: ClientCacheProxy[K, V] = getClientCacheProxy(cacheName, client)
      iterator.grouped(writeBatchSize).foreach((kv) => cache.putAll(mapAsJavaMap(kv.toMap)))
    }
  }

  private class HazelcastWriteToMapJob(val writeBatchSize: Int) extends Serializable {
    def runJob(ctx: TaskContext, iterator: Iterator[(K, V)], mapName: String, server: String): Unit = {
      val client: HazelcastInstance = getHazelcastConnection(server);
      val map: ClientMapProxy[K, V] = getClientMapProxy(mapName, client)

      iterator.grouped(writeBatchSize).foreach((kv) => map.putAll(mapAsJavaMap(kv.toMap)))
    }
  }

}
