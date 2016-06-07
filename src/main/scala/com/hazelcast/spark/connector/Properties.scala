package com.hazelcast.spark.connector

import org.apache.spark.SparkContext

import scala.util.Try

object Properties {

  val READ_BATCH_SIZE_PROP: String = "hazelcast.spark.connector.readBatchSize"
  val WRITE_BATCH_SIZE_PROP: String = "hazelcast.spark.connector.writeBatchSize"
  val BATCH_VALUES_PROP: String = "hazelcast.batch.values"
  val SERVER_ADDRESS_PROP: String = "hazelcast.server.address"

  def getReadBatchSize(sc: SparkContext): Int = {
    Try(sc.getConf.get(READ_BATCH_SIZE_PROP).toInt).getOrElse(1000)
  }

  def getWriteBatchSize(sc: SparkContext): Int = {
    Try(sc.getConf.get(WRITE_BATCH_SIZE_PROP).toInt).getOrElse(1000)
  }

  def isBatchingEnabled(sc: SparkContext): Boolean = {
    Try(sc.getConf.get(BATCH_VALUES_PROP).toBoolean).getOrElse(true)
  }

  def getServerAddress(sc: SparkContext): String = {
    sc.getConf.get(SERVER_ADDRESS_PROP)
  }


}
