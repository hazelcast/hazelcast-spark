package com.hazelcast.spark.connector.rdd

import com.hazelcast.client.cache.impl.ClientCacheProxy
import com.hazelcast.client.proxy.ClientMapProxy
import com.hazelcast.core.{HazelcastInstance, Partition => HazelcastPartition}
import com.hazelcast.spark.connector.conf.SerializableConf
import com.hazelcast.spark.connector.iterator.{CacheIterator, MapIterator}
import com.hazelcast.spark.connector.util.ConnectionUtil.{closeHazelcastConnection, getHazelcastConnection}
import com.hazelcast.spark.connector.util.HazelcastUtil._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.collection.JavaConversions._


class HazelcastRDD[K, V](@transient val sc: SparkContext, val hzName: String,
                         val isCache: Boolean, val config: SerializableConf) extends RDD[(K, V)](sc, Seq.empty) {

  @transient lazy val hazelcastPartitions: scala.collection.mutable.Map[Int, String] = {
    val client: HazelcastInstance = getHazelcastConnection(config.serverAddresses, config)
    val partitions: scala.collection.mutable.Map[Int, String] = scala.collection.mutable.Map[Int, String]()
    client.getPartitionService.getPartitions.foreach { p =>
      partitions.put(p.getPartitionId, p.getOwner.getAddress.getHost + ":" + p.getOwner.getAddress.getPort)
    }
    closeHazelcastConnection(config.serverAddresses)
    partitions
  }

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[(K, V)] = {
    val partitionLocationInfo = split.asInstanceOf[PartitionLocationInfo]
    val client: HazelcastInstance = getHazelcastConnection(partitionLocationInfo.location, config)
    if (isCache) {
      val cache: ClientCacheProxy[K, V] = getClientCacheProxy(hzName, client)
      new CacheIterator[K, V](cache.iterator(config.readBatchSize, split.index, config.valueBatchingEnabled))
    } else {
      val map: ClientMapProxy[K, V] = getClientMapProxy(hzName, client)
      new MapIterator[K, V](map.iterator(config.readBatchSize, split.index, config.valueBatchingEnabled))
    }
  }


  override protected def getPartitions: Array[Partition] = {
    var array: Array[Partition] = Array[Partition]()
    for (i <- 0 until hazelcastPartitions.size) {
      array = array :+ new PartitionLocationInfo(i, hazelcastPartitions.get(i).get)
    }
    array
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    Seq(hazelcastPartitions.get(split.index).get)
  }

}

