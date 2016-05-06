
import com.hazelcast.cache.impl.CacheProxy
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.spark.connector.{HazelcastHelper, HazelcastRDD}
import com.hazelcast.test.HazelcastTestSupport
import com.hazelcast.test.HazelcastTestSupport.randomName
import connector.toSparkContextFunctions
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Assert._
import org.junit._

class ReadFromHazelcastTest extends HazelcastTestSupport {

  var sparkContext: SparkContext = null
  var hazelcastInstance: HazelcastInstance = null

  @Before
  def before(): Unit = {
    System.setProperty("hazelcast.test.use.network", "true")
    System.setProperty("hazelcast.local.localAddress", "127.0.0.1")
    hazelcastInstance = createHazelcastInstance
    sparkContext = getSparkContext
  }

  @After
  def after(): Unit = {
    System.clearProperty("hazelcast.test.use.network")
    System.clearProperty("hazelcast.local.localAddress")
    hazelcastInstance.getLifecycleService.terminate()
    sparkContext.stop()
  }

  @Test
  def count(): Unit = {
    val hazelcastRDD: HazelcastRDD[Int, Int] = getPrepopulatedRDD
    val tuples: Array[(Int, Int)] = hazelcastRDD.collect()

    assertEquals("Count should be ", 100, tuples.length)
  }

  @Test
  def sortByKey(): Unit = {
    val hazelcastRDD: HazelcastRDD[Int, Int] = getPrepopulatedRDD
    val first = hazelcastRDD.sortByKey().first()

    assertEquals("First item should be", 1, first._1)
  }

  @Test
  def countByKey(): Unit = {
    val hazelcastRDD: HazelcastRDD[Int, Int] = getPrepopulatedRDD
    val map = hazelcastRDD.countByKey()

    assertTrue("All keys should have one value", map.forall({ case (k, v) => v == 1 }))
  }

  @Test
  def filter(): Unit = {
    val hazelcastRDD: HazelcastRDD[Int, Int] = getPrepopulatedRDD
    val filteredRDD = hazelcastRDD.filter { case (_, v) => v < 10 }

    assertTrue("All values should be less than 10", filteredRDD.values.collect().forall(_ < 10))
  }

  @Test
  def min(): Unit = {
    val hazelcastRDD: HazelcastRDD[Int, Int] = getPrepopulatedRDD
    val min = hazelcastRDD.min()

    assertEquals("min key should be one", 1, min._1)
    assertEquals("min value should be one", 1, min._2)
  }

  @Test
  def max(): Unit = {
    val hazelcastRDD: HazelcastRDD[Int, Int] = getPrepopulatedRDD
    val max = hazelcastRDD.max()

    assertEquals("max key should be 100", 100, max._1)
    assertEquals("max value should be 100", 100, max._2)
  }

  def getPrepopulatedRDD: HazelcastRDD[Int, Int] = {
    val cacheName: String = randomName()
    val cache: CacheProxy[Int, Int] = HazelcastHelper.getCacheFromServerProvider(cacheName, hazelcastInstance)
    for (i <- 1 to 100) {
      cache.put(i, i)
    }
    val hazelcastRDD: HazelcastRDD[Int, Int] = sparkContext.fromHazelcastCache(cacheName)
    hazelcastRDD
  }


  def getSparkContext: SparkContext = {
    val conf: SparkConf = new SparkConf().setMaster("local[8]").setAppName(this.getClass.getName)
      .set("spark.driver.host", "127.0.0.1")
      .set("hazelcast.server.address", "127.0.0.1:5701")
    new SparkContext(conf)
  }

}
