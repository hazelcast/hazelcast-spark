package com.hazelcast.spark.connector.conf

import com.hazelcast.spark.connector.conf.ConfigurationProperties._
import org.apache.spark.SparkContext

class SerializableConf(sc: SparkContext) extends Serializable {

  val serverAddresses: String = getServerAddress(sc)
  val xmlPath: String = getClientXmlPath(sc)
  val groupName: String = getServerGroupName(sc)
  val groupPass: String = getServerGroupPass(sc)
  val readBatchSize: Int = getReadBatchSize(sc)
  val writeBatchSize: Int = getWriteBatchSize(sc)
  val valueBatchingEnabled: Boolean = isValueBatchingEnabled(sc)


  override def toString = s"SerializableConf(serverAddresses=$serverAddresses, xmlPath=$xmlPath, groupName=$groupName, groupPass=$groupPass, readBatchSize=$readBatchSize, writeBatchSize=$writeBatchSize, valueBatchingEnabled=$valueBatchingEnabled)"
}
