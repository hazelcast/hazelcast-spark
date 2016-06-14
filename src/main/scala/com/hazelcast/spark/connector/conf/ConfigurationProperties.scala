package com.hazelcast.spark.connector.conf

import org.apache.spark.SparkContext

import scala.util.Try

object ConfigurationProperties {

  val READ_BATCH_SIZE_PROP: String = "hazelcast.spark.readBatchSize"
  val WRITE_BATCH_SIZE_PROP: String = "hazelcast.spark.writeBatchSize"
  val BATCH_VALUES_PROP: String = "hazelcast.spark.valueBatchingEnabled"
  val SERVER_ADDRESS_PROP: String = "hazelcast.server.addresses"
  val SERVER_GROUP_NAME_PROP: String = "hazelcast.server.group.name"
  val SERVER_GROUP_PASS_PROP: String = "hazelcast.server.group.pass"
  val CLIENT_XML_PATH_PROP: String = "hazelcast.spark.clientXmlPath"

  def getReadBatchSize(sc: SparkContext): Int = {
    Try(sc.getConf.get(READ_BATCH_SIZE_PROP).toInt).getOrElse(1000)
  }

  def getWriteBatchSize(sc: SparkContext): Int = {
    Try(sc.getConf.get(WRITE_BATCH_SIZE_PROP).toInt).getOrElse(1000)
  }

  def isValueBatchingEnabled(sc: SparkContext): Boolean = {
    Try(sc.getConf.get(BATCH_VALUES_PROP).toBoolean).getOrElse(true)
  }

  def getServerAddress(sc: SparkContext): String = {
    sc.getConf.get(SERVER_ADDRESS_PROP)
  }

  def getServerGroupName(sc: SparkContext): String = {
    Try(sc.getConf.get(SERVER_GROUP_NAME_PROP)).getOrElse("dev")
  }

  def getServerGroupPass(sc: SparkContext): String = {
    Try(sc.getConf.get(SERVER_GROUP_PASS_PROP)).getOrElse("dev-pass")
  }

  def getClientXmlPath(sc: SparkContext): String = {
    Try(sc.getConf.get(CLIENT_XML_PATH_PROP)).getOrElse(null)
  }

}
