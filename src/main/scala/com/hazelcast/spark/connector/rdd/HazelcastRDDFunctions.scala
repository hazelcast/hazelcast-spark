package com.hazelcast.spark.connector.rdd

import com.hazelcast.client.HazelcastClientNotActiveException
import com.hazelcast.client.cache.impl.ClientCacheProxy
import com.hazelcast.client.proxy.ClientMapProxy
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.spark.connector.conf.SerializableConf
import com.hazelcast.spark.connector.util.CleanupUtil.addCleanupListener
import com.hazelcast.spark.connector.util.ConnectionUtil._
import com.hazelcast.spark.connector.util.HazelcastUtil.{getClientCacheProxy, getClientMapProxy}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.util.Try

class HazelcastRDDFunctions[K, V](val rdd: RDD[(K, V)]) extends Serializable {
  val conf: SerializableConf = new SerializableConf(rdd.context)

  def saveToHazelcastCache(cacheName: String): Unit = {
    val job = (ctx: TaskContext, iterator: Iterator[(K, V)]) => {
      new HazelcastWriteToCacheJob().runJob(ctx, iterator, cacheName)
    }
    addCleanupListener(rdd.context)
    rdd.sparkContext.runJob(rdd, job)

  }

  def saveToHazelcastMap(mapName: String): Unit = {
    val job = (ctx: TaskContext, iterator: Iterator[(K, V)]) => {
      new HazelcastWriteToMapJob().runJob(ctx, iterator, mapName)
    }
    addCleanupListener(rdd.context)
    rdd.sparkContext.runJob(rdd, job)
  }

  private class HazelcastWriteToCacheJob extends Serializable {
    def runJob(ctx: TaskContext, iterator: Iterator[(K, V)], cacheName: String): Unit = {
      Try(writeInternal(iterator, cacheName)).recover({
        case e: HazelcastClientNotActiveException ⇒ writeInternal(iterator, cacheName)
      })
    }

    def writeInternal(iterator: Iterator[(K, V)], cacheName: String): Unit = {
      val client: HazelcastInstance = getHazelcastConnection(conf.serverAddresses, rdd.id, conf)
      val cache: ClientCacheProxy[K, V] = getClientCacheProxy(cacheName, client)
      iterator.grouped(conf.writeBatchSize).foreach((kv) => cache.putAll(mapAsJavaMap(kv.toMap)))
    }
  }

  private class HazelcastWriteToMapJob extends Serializable {
    def runJob(ctx: TaskContext, iterator: Iterator[(K, V)], mapName: String): Unit = {
      Try(writeInternal(iterator, mapName)).recover({
        case e: HazelcastClientNotActiveException ⇒ writeInternal(iterator, mapName)
      })
    }

    def writeInternal(iterator: Iterator[(K, V)], mapName: String): Unit = {
      val client: HazelcastInstance = getHazelcastConnection(conf.serverAddresses, rdd.id, conf)
      val map: ClientMapProxy[K, V] = getClientMapProxy(mapName, client)
      iterator.grouped(conf.writeBatchSize).foreach((kv) => map.putAll(mapAsJavaMap(kv.toMap)))
    }
  }

}
