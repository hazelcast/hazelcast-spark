package com.hazelcast.spark.connector

import com.hazelcast.client.HazelcastClient
import com.hazelcast.client.config.ClientConfig
import com.hazelcast.core.HazelcastInstance

import scala.collection.mutable
import scala.collection.JavaConversions

object ConnectionManager {

  private[connector] val instances = mutable.Map[String, HazelcastInstance]()

  def getHazelcastConnection(member: String): HazelcastInstance = {
    this.synchronized {
      val maybeInstance: Option[HazelcastInstance] = instances.get(member)
      if (maybeInstance.isEmpty) {
        val config: ClientConfig = new ClientConfig
        config.getNetworkConfig.setAddresses(JavaConversions.seqAsJavaList(Seq(member)))
        val client: HazelcastInstance = HazelcastClient.newHazelcastClient(config)
        instances.put(member, client)
        client
      } else {
        maybeInstance.get
      }
    }
  }

  def closeHazelcastConnection(member: String): Unit = {
    this.synchronized {
      val maybeInstance: Option[HazelcastInstance] = instances.get(member)
      if (maybeInstance.isDefined) {
        val instance: HazelcastInstance = maybeInstance.get
        instance.getLifecycleService.shutdown()
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


}
