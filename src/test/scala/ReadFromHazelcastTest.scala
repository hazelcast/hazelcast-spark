
import com.hazelcast.cache.impl.CacheProxy
import com.hazelcast.client.cache.impl.ClientCacheProxy
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.spark.connector.{HazelcastHelper, HazelcastRDD}
import com.hazelcast.test.HazelcastTestSupport
import com.hazelcast.test.HazelcastTestSupport.randomName
import connector.toSparkContextFunctions
import org.apache.spark.{SparkConf, SparkContext}
import org.junit._

class ReadFromHazelcastTest extends HazelcastTestSupport {

  @Test
  def readFromHazelcast(): Unit = {
    System.setProperty("hazelcast.test.use.network", "true")
    System.setProperty("hazelcast.local.localAddress", "127.0.0.1")
    val instance: HazelcastInstance = createHazelcastInstance()

    val cacheName: String = randomName()
    val cache: CacheProxy[Int, Int] = HazelcastHelper.getCacheFromServerProvider(cacheName, instance)
    for (i <- 1 to 100) {
      cache.put(i, i)
    }

    val conf: SparkConf = new SparkConf().setMaster("local[8]").setAppName(this.getClass.getName)
      .set("spark.driver.host", "127.0.0.1")
      .set("hazelcast.server.address", "127.0.0.1:5701")
    val sc: SparkContext = new SparkContext(conf)

    val hazelcastRDD: HazelcastRDD[Int, Int] = sc.fromHazelcastCache(cacheName)
    val tuples: Array[(Int, Int)] = hazelcastRDD.collect()

    Assert.assertEquals("", 100, tuples.length)

  }
}
