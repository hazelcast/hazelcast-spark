import com.hazelcast.cache.impl.CacheProxy;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.spark.connector.util.HazelcastUtil;
import com.hazelcast.test.HazelcastTestSupport;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.hazelcast.spark.connector.HazelcastJavaPairRDDFunctions.javaPairRddFunctions;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class WriteToHazelcastJavaTest extends HazelcastTestSupport {

    public static final String GROUP_NAME = randomName();

    @Parameterized.Parameter
    public boolean toCache;

    @Parameterized.Parameters(name = "toCache:{0}")
    public static Iterable<Object[]> parameters() {
        return Arrays.asList(new Object[]{Boolean.TRUE}, new Object[]{Boolean.FALSE});
    }

    HazelcastInstance server;
    JavaSparkContext sparkContext;

    @Before
    public void setUp() throws Exception {
        System.setProperty("hazelcast.test.use.network", "true");
        System.setProperty("hazelcast.local.localAddress", "127.0.0.1");
        Config config = getConfig();
        config.getGroupConfig().setName(GROUP_NAME);
        server = createHazelcastInstance(config);

        SparkConf conf = new SparkConf().setMaster("local[8]").setAppName(this.getClass().getName())
                .set("hazelcast.server.addresses", "127.0.0.1:5701")
                .set("hazelcast.server.groupName", GROUP_NAME)
                .set("spark.driver.host", "127.0.0.1");
        sparkContext = new JavaSparkContext(conf);
    }

    @After
    public void tearDown() throws Exception {
        server.getLifecycleService().terminate();
        while (HazelcastInstanceFactory.getAllHazelcastInstances().size() > 0) {
            sleepMillis(50);
        }
        sparkContext.stop();
    }

    @Test
    public void zipWithIndex() throws Exception {
        JavaPairRDD<Object, Long> pairRDD = sparkContext.parallelize(new ArrayList<Object>() {{
            add(1);
            add(2);
            add(3);
            add(4);
            add(5);
        }}).zipWithIndex();
        String name = randomMapName();
        saveToHazelcast(pairRDD, name);

        assertSize(name, 5);
    }


    @Test
    public void mapValues() throws Exception {
        JavaPairRDD<Integer, Long> pairRDD = sparkContext.parallelize(new ArrayList<Integer>() {{
            add(1);
            add(2);
            add(3);
            add(4);
            add(5);
        }}).zipWithIndex();
        String name = randomMapName();
        JavaPairRDD<Integer, Object> mapValues = pairRDD.mapValues(new MapValueTo5());
        saveToHazelcast(mapValues, name);

        assertSize(name, 5);
        assertValue(name, 5, 5);
    }

    private void assertSize(String name, int size) {
        if (toCache) {
            CacheProxy<Object, Object> cache = HazelcastUtil.getServerCacheProxy(name, server);
            assertEquals("Cache size should be " + size, size, cache.size());
        } else {
            MapProxyImpl<Object, Object> map = HazelcastUtil.getServerMapProxy(name, server);
            assertEquals("Map size should be " + size, size, map.size());
        }
    }

    private void assertValue(String name, int size, int value) {
        if (toCache) {
            CacheProxy<Object, Object> cache = HazelcastUtil.getServerCacheProxy(name, server);
            for (int i = 1; i <= size; i++) {
                assertEquals(value, cache.get(i));
            }
        } else {
            MapProxyImpl<Object, Object> map = HazelcastUtil.getServerMapProxy(name, server);
            for (int i = 1; i <= size; i++) {
                assertEquals(value, map.get(i));
            }
        }
    }

    private <K, V> void saveToHazelcast(JavaPairRDD<K, V> rdd, String name) {
        if (toCache) {
            javaPairRddFunctions(rdd).saveToHazelcastCache(name);
        } else {
            javaPairRddFunctions(rdd).saveToHazelcastMap(name);
        }
    }


    static class MapValueTo5 implements Function<Long, Object> {
        @Override
        public Object call(Long v1) throws Exception {
            return 5;
        }
    }


}
