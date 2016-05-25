package com.hazelcast.spark.connector

import com.hazelcast.client.cache.impl.ClientCacheProxy
import com.hazelcast.client.proxy.ClientMapProxy
import com.hazelcast.core.{HazelcastInstance, Partition => HazelcastPartition}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.collection.JavaConversions._
import scala.util.Try


class HazelcastRDD[K, V](@transient val sc: SparkContext, val hzName: String,
                         val isCache: Boolean, val server: String, val prefetchValues: Boolean) extends RDD[(K, V)](sc, Seq.empty) {

  val iteratorFetchSize: Int = Try(sc.getConf.get("hazelcast.spark.connector.readBatchSize").toInt).getOrElse(1000)

  @transient lazy val hazelcastPartitions: scala.collection.mutable.Map[Int, String] = {
    val client: HazelcastInstance = ConnectionManager.getHazelcastConnection(server)
    val partitions: scala.collection.mutable.Map[Int, String] = scala.collection.mutable.Map[Int, String]()
    client.getPartitionService.getPartitions.foreach { p =>
      partitions.put(p.getPartitionId, p.getOwner.getAddress.getHost + ":" + p.getOwner.getAddress.getPort)
    }
    ConnectionManager.closeHazelcastConnection(server)
    partitions
  }

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[(K, V)] = {
    val partitionLocationInfo = split.asInstanceOf[PartitionLocationInfo]
    val client: HazelcastInstance = ConnectionManager.getHazelcastConnection(partitionLocationInfo.location)
    if (isCache) {
      val cache: ClientCacheProxy[K, V] = HazelcastHelper.getClientCacheProxy(hzName, client)
      new CacheIterator[K, V](cache.iterator(iteratorFetchSize, split.index, prefetchValues))
    } else {
      val map: ClientMapProxy[K, V] = HazelcastHelper.getClientMapProxy(hzName, client)
      new MapIterator[K, V](map.iterator(iteratorFetchSize, split.index, prefetchValues))
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

