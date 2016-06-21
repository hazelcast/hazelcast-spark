package com.hazelcast.spark.connector.util

import com.hazelcast.client.HazelcastClient
import com.hazelcast.client.config.{ClientConfig, XmlClientConfigBuilder}
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.spark.connector.conf.SerializableConf

import scala.collection.{JavaConversions, mutable}

object ConnectionUtil {

  private[connector] val instances = mutable.Map[String, HazelcastInstance]()

  def getHazelcastConnection(member: String, conf: SerializableConf): HazelcastInstance = {
    def createClientInstance: HazelcastInstance = {
      val client: HazelcastInstance = HazelcastClient.newHazelcastClient(createClientConfig(conf, member))
      instances.put(member, client)
      client
    }
    this.synchronized {
      val maybeInstance: Option[HazelcastInstance] = instances.get(member)
      if (maybeInstance.isEmpty) {
        createClientInstance
      } else {
        val instance: HazelcastInstance = maybeInstance.get
        if (instance.getLifecycleService.isRunning) {
          instance
        } else {
          createClientInstance
        }
      }
    }
  }

  def closeHazelcastConnection(member: String): Unit = {
    this.synchronized {
      val maybeInstance: Option[HazelcastInstance] = instances.get(member)
      if (maybeInstance.isDefined) {
        val instance: HazelcastInstance = maybeInstance.get
        if (instance.getLifecycleService.isRunning) {
          instance.getLifecycleService.shutdown()
        }
        instances.remove(member)
      }
    }
  }

  def closeAll(): Unit = {
    this.synchronized {
      instances.values.foreach(instance => instance.getLifecycleService.shutdown())
      instances.clear()
    }
  }

  private def createClientConfig(conf: SerializableConf, member: String): ClientConfig = {
    var config: ClientConfig = null
    if (conf.xmlPath != null) {
      config = new XmlClientConfigBuilder(conf.xmlPath).build()
    } else {
      config = new ClientConfig
      config.getGroupConfig.setName(conf.groupName)
      config.getGroupConfig.setPassword(conf.groupPass)
      config.getNetworkConfig.setAddresses(JavaConversions.seqAsJavaList(member.split(",")))
    }
    config
  }

}
