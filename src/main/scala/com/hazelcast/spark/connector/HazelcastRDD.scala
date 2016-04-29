package com.hazelcast.spark.connector

import com.hazelcast.client.cache.impl.ClientCacheProxy
import com.hazelcast.core.{HazelcastInstance, Partition => HazelcastPartition}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.collection.JavaConversions._


class HazelcastRDD[K, V](@transient val sc: SparkContext, val cacheName: String, val server: String) extends RDD[(K, V)](sc, Seq.empty) {

  @transient lazy val hazelcastPartitions: scala.collection.mutable.Map[Int, String] = {
    val client: HazelcastInstance = ConnectionManager.getHazelcastConnection(server)
    val partitions: scala.collection.mutable.Map[Int, String] = scala.collection.mutable.Map[Int, String]()
    client.getPartitionService.getPartitions.foreach { p =>
      partitions.put(p.getPartitionId, p.getOwner.getAddress.getHost + ":" + p.getOwner.getAddress.getPort)
    }
    ConnectionManager.closeHazelcastConnection(server)
    partitions
  }


  override protected def clearDependencies(): Unit = super.clearDependencies()

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[(K, V)] = {
    val partitionLocationInfo = split.asInstanceOf[PartitionLocationInfo]
    val client: HazelcastInstance = ConnectionManager.getHazelcastConnection(partitionLocationInfo.location)
    val cache: ClientCacheProxy[K, V] = HazelcastHelper.getCacheFromClientProvider(cacheName, client)
    new CacheIterator[K, V](cache.iterator(100, split.index))
  }

  override protected def getPartitions: Array[Partition] = {
    var array: Array[Partition] = Array[Partition]()
    for (i <- 0 to 270) {
      array = array :+ new PartitionLocationInfo(i, hazelcastPartitions.get(i).get)
    }
    array
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    val location: String = hazelcastPartitions.get(split.index).get
    Seq(location)
  }

}

