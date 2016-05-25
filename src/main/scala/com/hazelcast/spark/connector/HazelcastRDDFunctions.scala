package com.hazelcast.spark.connector

import com.hazelcast.client.cache.impl.ClientCacheProxy
import com.hazelcast.client.proxy.ClientMapProxy
import com.hazelcast.core.HazelcastInstance
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD


class HazelcastRDDFunctions[K, V](val rdd: RDD[(K, V)]) extends Serializable {

  def saveToHazelcastCache(cacheName: String): Unit = {
    val server: String = rdd.sparkContext.getConf.get("hazelcast.server.address")
    val job = (ctx: TaskContext, iterator: Iterator[(K, V)]) => {
      new HazelcastWriteToCacheJob().runJob(ctx, iterator, cacheName, server)
    }
    rdd.sparkContext.runJob(rdd, job)
  }

  def saveToHazelcastMap(mapName: String): Unit = {
    val server: String = rdd.sparkContext.getConf.get("hazelcast.server.address")
    val job = (ctx: TaskContext, iterator: Iterator[(K, V)]) => {
      new HazelcastWriteToMapJob().runJob(ctx, iterator, mapName, server)
    }
    rdd.sparkContext.runJob(rdd, job)
  }

  private class HazelcastWriteToCacheJob() extends Serializable {
    def runJob(ctx: TaskContext, iterator: Iterator[(K, V)], cacheName: String, server: String): Unit = {
      val client: HazelcastInstance = ConnectionManager.getHazelcastConnection(server);
      val cache: ClientCacheProxy[K, V] = HazelcastHelper.getClientCacheProxy(cacheName, client)
      iterator.foreach((kv) => cache.put(kv._1, kv._2))
    }
  }

  private class HazelcastWriteToMapJob() extends Serializable {
    def runJob(ctx: TaskContext, iterator: Iterator[(K, V)], mapName: String, server: String): Unit = {
      val client: HazelcastInstance = ConnectionManager.getHazelcastConnection(server);
      val map: ClientMapProxy[K, V] = HazelcastHelper.getClientMapProxy(mapName, client)
      iterator.foreach((kv) => map.put(kv._1, kv._2))
    }
  }

}
