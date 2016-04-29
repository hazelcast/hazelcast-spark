import com.hazelcast.cache.impl.CacheProxy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spark.connector.HazelcastHelper;
import com.hazelcast.spark.connector.HazelcastRDD;
import com.hazelcast.test.HazelcastTestSupport;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.junit.Assert;
import org.junit.Test;

public class ReadFromHazelcastJavaTest extends HazelcastTestSupport {

    @Test
    public void readFromHazelcast() {
        System.setProperty("hazelcast.test.use.network", "true");
        System.setProperty("hazelcast.local.localAddress", "127.0.0.1");
        HazelcastInstance instance = createHazelcastInstance();

        String cacheName = randomName();
        CacheProxy<Integer, Integer> cache = HazelcastHelper.getCacheFromServerProvider(cacheName, instance);
        for (int i = 0; i < 100; i++) {
            cache.put(i, i);
        }

        SparkConf conf = new SparkConf().setMaster("local[8]").setAppName(this.getClass().getName()).set("spark.driver.host", "127.0.0.1");
        HazelcastSparkContext sc = new HazelcastSparkContext(new SparkContext(conf));

        HazelcastRDD<Integer, Integer> hazelcastRDD = sc.fromHazelcastCache(cacheName);
        Assert.assertEquals("", 100, hazelcastRDD.count());
    }

}
