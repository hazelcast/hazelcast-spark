package com.hazelcast.spark.connector


import com.hazelcast.client.config.ClientConfig
import com.hazelcast.client.{HazelcastClient, HazelcastClientManager}
import com.hazelcast.config.Config
import com.hazelcast.core.{HazelcastInstance, IMap}
import com.hazelcast.test.HazelcastTestSupport
import com.hazelcast.test.HazelcastTestSupport._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{After, Before, Test}

import scala.collection.{JavaConversions, Map}

@RunWith(value = classOf[Parameterized])
class WriteToHazelcastPerformanceTest(sparkWriteBatchSize: Int) extends HazelcastTestSupport {

  var sparkContext: SparkContext = null
  var hazelcastInstance: HazelcastInstance = null
  val groupName: String = randomName()
  val ITEM_COUNT: Int = 1000000


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
    hazelcastInstance.getLifecycleService.terminate()
  }


  @Test
  def sparkWrite(): Unit = {
    val name: String = randomName()
    val rdd: RDD[(Int, Long)] = sparkContext.parallelize(1 to ITEM_COUNT).zipWithIndex()
    val map: Map[Int, Long] = rdd.collectAsMap()
    val javaMap: java.util.Map[Int, Long] = JavaConversions.mapAsJavaMap(map)


    val startSpark = System.currentTimeMillis
    rdd.saveToHazelcastMap(name)
    val endSpark = System.currentTimeMillis
    val tookSpark = endSpark - startSpark

    println("write via spark took : " + tookSpark)
    stopSpark
  }


  @Test
  def hazelcastWrite(): Unit = {
    val name: String = randomName()
    val map: java.util.Map[Int, Int] = JavaConversions.mapAsJavaMap((1 to ITEM_COUNT).zipWithIndex.toMap)
    val config: ClientConfig = new ClientConfig
    config.getGroupConfig.setName(groupName)
    val client: HazelcastInstance = HazelcastClient.newHazelcastClient(config)

    val hazelcastMap: IMap[Int, Int] = client.getMap(name)
    val startHz = System.currentTimeMillis
    hazelcastMap.putAll(map)
    val endHz = System.currentTimeMillis
    val tookHz = endHz - startHz

    println("write via hazelcast took : " + tookHz)
    client.getLifecycleService.shutdown()
    stopSpark
  }


  def stopSpark: Unit = {
    while (HazelcastClientManager.getAllHazelcastClients.size > 0) {
      sleepMillis(50)
    }
    sparkContext.stop()
  }

  def getSparkContext: SparkContext = {
    val conf: SparkConf = new SparkConf().setMaster("local[8]").setAppName(this.getClass.getName)
      .set("spark.driver.host", "127.0.0.1")
      .set("hazelcast.server.addresses", "127.0.0.1:5701")
      .set("hazelcast.server.groupName", groupName)
      .set("hazelcast.spark.writeBatchSize", sparkWriteBatchSize.toString)
    new SparkContext(conf)
  }


}

object WriteToHazelcastPerformanceTest {
  @Parameterized.Parameters(name = "sparkWriteBatchSize = {0}") def parameters: java.lang.Iterable[Array[AnyRef]] = {
    java.util.Arrays.asList(Array(Int.box(1000)), Array(Int.box(10000)), Array(Int.box(100000)), Array(Int.box(1000000)))
  }
}

