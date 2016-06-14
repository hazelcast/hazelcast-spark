package com.hazelcast.spark.connector.rdd

import org.apache.spark.Partition

class PartitionLocationInfo(val partitionId: Int, val location: String) extends Partition {
  override def index: Int = partitionId
}
