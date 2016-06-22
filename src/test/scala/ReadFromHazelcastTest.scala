
import com.hazelcast.cache.impl.CacheProxy
import com.hazelcast.client.HazelcastClientManager
import com.hazelcast.config.Config
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.map.impl.proxy.MapProxyImpl
import com.hazelcast.spark.connector.rdd.HazelcastRDD
import com.hazelcast.spark.connector.toSparkContextFunctions
import com.hazelcast.spark.connector.util.HazelcastUtil
import com.hazelcast.test.HazelcastTestSupport
import com.hazelcast.test.HazelcastTestSupport._
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Assert._
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

@RunWith(value = classOf[Parameterized])
class ReadFromHazelcastTest(fromCache: Boolean) extends HazelcastTestSupport {

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
  def count(): Unit = {
    val hazelcastRDD: HazelcastRDD[Int, Int] = getPrepopulatedRDD()
    val tuples: Array[(Int, Int)] = hazelcastRDD.collect()

    assertEquals("Count should be ", 100, tuples.length)
  }

  @Test
  def isEmpty(): Unit = {
    val hazelcastRDD: HazelcastRDD[Int, Int] = getPrepopulatedRDD()
    assertFalse(hazelcastRDD.isEmpty())
  }


  @Test
  def sortByKey(): Unit = {
    val hazelcastRDD: HazelcastRDD[Int, Int] = getPrepopulatedRDD()
    val first = hazelcastRDD.sortByKey().first()

    assertEquals("First item should be", 1, first._1)
  }

  @Test
  def countByKey(): Unit = {
    val hazelcastRDD: HazelcastRDD[Int, Int] = getPrepopulatedRDD()
    val map = hazelcastRDD.countByKey()

    assertTrue("All keys should have one value", map.forall({ case (k, v) => v == 1 }))
  }

  @Test
  def countByValue(): Unit = {
    val hazelcastRDD: HazelcastRDD[Int, Int] = getPrepopulatedRDD()
    val map = hazelcastRDD.countByValue()

    assertTrue("All values should appear once", map.forall({ case (k, v) => v == 1 }))
  }

  @Test
  def filter(): Unit = {
    val hazelcastRDD: HazelcastRDD[Int, Int] = getPrepopulatedRDD()
    val filteredRDD = hazelcastRDD.filter { case (_, v) => v < 10 }

    assertTrue("All values should be less than 10", filteredRDD.values.collect().forall(_ < 10))
  }

  @Test
  def min(): Unit = {
    val hazelcastRDD: HazelcastRDD[Int, Int] = getPrepopulatedRDD()
    val min = hazelcastRDD.min()

    assertEquals("min key should be one", 1, min._1)
    assertEquals("min value should be one", 1, min._2)
  }

  @Test
  def max(): Unit = {
    val hazelcastRDD: HazelcastRDD[Int, Int] = getPrepopulatedRDD()
    val max = hazelcastRDD.max()

    assertEquals("max key should be 100", 100, max._1)
    assertEquals("max value should be 100", 100, max._2)
  }

  @Test
  def flatMap(): Unit = {
    val hazelcastRDD: HazelcastRDD[Int, Int] = getPrepopulatedRDD()
    val values = hazelcastRDD.flatMap(e => List(e._2)).collect()

    assertEquals(100, values.length)
  }

  def getPrepopulatedRDD(): HazelcastRDD[Int, Int] = {
    val name: String = randomName()
    if (fromCache) {
      val cache: CacheProxy[Int, Int] = HazelcastUtil.getServerCacheProxy(name, hazelcastInstance)
      for (i <- 1 to 100) {
        cache.put(i, i)
      }
      val hazelcastRDD: HazelcastRDD[Int, Int] = sparkContext.fromHazelcastCache(name)
      hazelcastRDD

    } else {
      val map: MapProxyImpl[Int, Int] = HazelcastUtil.getServerMapProxy(name, hazelcastInstance)
      for (i <- 1 to 100) {
        map.put(i, i)
      }
      val hazelcastRDD: HazelcastRDD[Int, Int] = sparkContext.fromHazelcastMap(name)
      hazelcastRDD
    }
  }


  def getSparkContext: SparkContext = {
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName(this.getClass.getName)
      .set("spark.driver.host", "127.0.0.1")
      .set("hazelcast.server.addresses", "127.0.0.1:5701")
      .set("hazelcast.server.groupName", groupName)
    new SparkContext(conf)
  }

}

object ReadFromHazelcastTest {
  @Parameterized.Parameters(name = "fromCache = {0}") def parameters: java.util.Collection[Array[AnyRef]] = {
    java.util.Arrays.asList(Array(Boolean.box(false)), Array(Boolean.box(true)))
  }
}

