package com.hazelcast.spark.connector

import java.io.BufferedWriter

import com.hazelcast.client.config.ClientConfig
import com.hazelcast.client.{HazelcastClient, HazelcastClientManager}
import com.hazelcast.config.Config
import com.hazelcast.core.{HazelcastInstance, IMap}
import com.hazelcast.spark.connector.rdd.HazelcastRDD
import com.hazelcast.test.HazelcastTestSupport
import com.hazelcast.test.HazelcastTestSupport._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Before, Ignore, Test}

import scala.collection.JavaConversions

class ReadPerformanceTest extends HazelcastTestSupport {

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
  def readFromSpark(): Unit = {
    val rdd: RDD[(Int, Long)] = sparkContext.parallelize(1 to ITEM_COUNT).zipWithIndex()
    rdd.persist()
    rdd.count()

    val startSpark = System.currentTimeMillis
    rdd.take(ITEM_COUNT)
    val endSpark = System.currentTimeMillis
    val tookSpark = endSpark - startSpark

//    val writer: BufferedWriter = scala.tools.nsc.io.File("/Users/emindemirci/Desktop/sparkResults").bufferedWriter(true)
//    writer.append(tookSpark.toString)
//    writer.newLine()
//    writer.close()

    println("read via spark took : " + tookSpark)
    stopSpark
  }

  @Test
  def readFromHazelcast(): Unit = {
    val name: String = randomName()
    val map: java.util.Map[Int, Int] = JavaConversions.mapAsJavaMap((1 to ITEM_COUNT).zipWithIndex.toMap)
    val config: ClientConfig = new ClientConfig
    config.getGroupConfig.setName(groupName)
    val client: HazelcastInstance = HazelcastClient.newHazelcastClient(config)
    val hazelcastMap: IMap[Int, Int] = client.getMap(name)
    hazelcastMap.putAll(map)
    client.getLifecycleService.shutdown()

    val startHz = System.currentTimeMillis
    val hazelcastRDD: HazelcastRDD[Nothing, Nothing] = sparkContext.fromHazelcastMap(name)
    hazelcastRDD.take(ITEM_COUNT)
    val endHz = System.currentTimeMillis
    val tookHz = endHz - startHz

//    val writer: BufferedWriter = scala.tools.nsc.io.File("/Users/emindemirci/Desktop/hazelcastResults").bufferedWriter(true)
//    writer.append(tookHz.toString)
//    writer.newLine()
//    writer.close()

    println("read via hazelcast took : " + tookHz)
    stopSpark
  }

  @Test
  @Ignore // since this requires local tachyon installation
  def readFromTachyon(): Unit = {
    val rdd: RDD[(Int, Long)] = sparkContext.parallelize(1 to ITEM_COUNT).zipWithIndex()
    rdd.persist(org.apache.spark.storage.StorageLevel.OFF_HEAP)
    rdd.count() // to actually persist to Tachyon

    val startSpark = System.currentTimeMillis
    rdd.take(ITEM_COUNT)
    val endSpark = System.currentTimeMillis
    val tookSpark = endSpark - startSpark

//    val writer: BufferedWriter = scala.tools.nsc.io.File("/Users/emindemirci/Desktop/tachyonResults").bufferedWriter(true)
//    writer.append(tookSpark.toString)
//    writer.newLine()
//    writer.close()

    println("read via tachyon took : " + tookSpark)
    stopSpark
  }


  def stopSpark: Unit = {
    while (HazelcastClientManager.getAllHazelcastClients.size > 0) {
      sleepMillis(50)
    }
    sparkContext.stop()
  }

  def getSparkContext: SparkContext = {
    val conf: SparkConf = new SparkConf().setMaster("local[1]").setAppName(this.getClass.getName)
      .set("spark.driver.host", "127.0.0.1")
      .set("hazelcast.server.addresses", "127.0.0.1:5701")
      .set("hazelcast.server.groupName", groupName)
    new SparkContext(conf)
  }


}



