package com.hazelcast.spark.connector.util

import com.hazelcast.client.HazelcastClient
import com.hazelcast.client.config.{ClientConfig, XmlClientConfigBuilder}
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.spark.connector.conf.SerializableConf

import scala.collection.{JavaConversions, mutable}

object ConnectionUtil {

  private[connector] val instances = mutable.Map[String, HazelcastInstance]()

  def getHazelcastConnection(member: String, rddId: Int, conf: SerializableConf): HazelcastInstance = {
    def createClientInstance: HazelcastInstance = {
      val client: HazelcastInstance = HazelcastClient.newHazelcastClient(createClientConfig(conf, member))
      instances.put(member + "#" + rddId, client)
      client
    }
    this.synchronized {
      val maybeInstance: Option[HazelcastInstance] = instances.get(member + "#" + rddId)
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

  def closeHazelcastConnection(member: String, rddId: Int): Unit = {
    this.synchronized {
      val maybeInstance: Option[HazelcastInstance] = instances.get(member + "#" + rddId)
      if (maybeInstance.isDefined) {
        val instance: HazelcastInstance = maybeInstance.get
        if (instance.getLifecycleService.isRunning) {
          instance.getLifecycleService.shutdown()
        }
        instances.remove(member + "#" + rddId)
      }
    }
  }

  def closeAll(rddIds: Seq[Int]): Unit = {
    this.synchronized {
      instances.keys.foreach({
        key => {
          val instanceRddId: String = key.split("#")(1)
          if (rddIds.contains(instanceRddId.toInt)) {
            val instance: HazelcastInstance = instances.get(key).get
            if (instance.getLifecycleService.isRunning) {
              instance.shutdown();
            }
            instances.remove(key);
          }
        }
      })
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
