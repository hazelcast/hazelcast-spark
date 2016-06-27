package com.hazelcast.spark.connector

import com.hazelcast.cache.impl.CacheProxy
import com.hazelcast.client.HazelcastClientManager
import com.hazelcast.config.Config
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.map.impl.proxy.MapProxyImpl
import com.hazelcast.spark.connector.util.HazelcastUtil
import com.hazelcast.test.HazelcastTestSupport
import com.hazelcast.test.HazelcastTestSupport._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Assert._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{After, Before, Test}


@RunWith(value = classOf[Parameterized])
class WriteToHazelcastTest(toCache: Boolean) extends HazelcastTestSupport {

  var sparkContext: SparkContext = null
  var hazelcastInstance: HazelcastInstance = null
  val groupName: String = randomName()

  @Before
  def before(): Unit = {
    System.setProperty("hazelcast.test.use.network", "true")
    System.setProperty("hazelcast.local.localAddress", "127.0.0.1")
    val config: Config = getConfig
    config.getGroupConfig.setName(groupName)
    hazelcastInstance = createHazelcastInstance(config)
    sparkContext = getSparkContext
  }

  @After
  def after(): Unit = {
    System.clearProperty("hazelcast.test.use.network")
    System.clearProperty("hazelcast.local.localAddress")
    while (HazelcastClientManager.getAllHazelcastClients.size > 0) {
      sleepMillis(50)
    }
    hazelcastInstance.getLifecycleService.terminate()
    sparkContext.stop()
  }


  @Test
  def zipWithIndex(): Unit = {
    val name: String = randomName()
    val rdd: RDD[(Int, Long)] = sparkContext.parallelize(1 to 1000).zipWithIndex()
    saveToHazelcast(rdd, name)

    assertSize(name, 1000)
  }


  @Test
  def mapValues(): Unit = {
    val name: String = randomName()
    val rdd: RDD[(Int, Long)] = sparkContext.parallelize(1 to 1000).zipWithIndex()
    val mapValues: RDD[(Int, Long)] = rdd.mapValues((value) => 20l)
    saveToHazelcast(mapValues, name)

    assertSize(name, 1000)
    assertValue(name, 1000, 20)
  }

  def assertSize(name: String, size: Int) = {
    if (toCache) {
      val cache: CacheProxy[Int, Long] = HazelcastUtil.getServerCacheProxy(name, hazelcastInstance)
      assertEquals("Cache size should be " + size, size, cache.size())
    } else {
      val map: MapProxyImpl[Int, Long] = HazelcastUtil.getServerMapProxy(name, hazelcastInstance)
      assertEquals("Map size should be " + size, size, map.size())
    }
  }

  def assertValue(name: String, size: Int, value: Int) = {
    if (toCache) {
      val cache: CacheProxy[Int, Long] = HazelcastUtil.getServerCacheProxy(name, hazelcastInstance)
      for (i <- 1 to size) {
        assertEquals(value, cache.get(i))
      }
    } else {
      val map: MapProxyImpl[Int, Long] = HazelcastUtil.getServerMapProxy(name, hazelcastInstance)
      for (i <- 1 to size) {
        assertEquals(value, map.get(i))
      }
    }
  }

  def saveToHazelcast(rdd: RDD[(Int, Long)], name: String) = {
    if (toCache) {
      rdd.saveToHazelcastCache(name)
    } else {
      rdd.saveToHazelcastMap(name)
    }
  }

  def getSparkContext: SparkContext = {
    val conf: SparkConf = new SparkConf().setMaster("local[8]").setAppName(this.getClass.getName)
      .set("spark.driver.host", "127.0.0.1")
      .set("hazelcast.server.addresses", "127.0.0.1:5701")
      .set("hazelcast.server.groupName", groupName)
    new SparkContext(conf)
  }

}

object WriteToHazelcastTest {

  @Parameterized.Parameters(name = "toCache = {0}") def parameters: java.util.Collection[Array[AnyRef]] = {
    java.util.Arrays.asList(Array(Boolean.box(false)), Array(Boolean.box(true)))
  }
}

