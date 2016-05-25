import com.hazelcast.cache.impl.CacheProxy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.spark.connector.HazelcastHelper;
import com.hazelcast.spark.connector.HazelcastJavaRDD;
import com.hazelcast.spark.connector.HazelcastSparkContext;
import com.hazelcast.test.HazelcastTestSupport;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import scala.Tuple2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class ReadFromHazelcastJavaTest extends HazelcastTestSupport {

    @Parameterized.Parameter
    public boolean fromCache;

    @Parameterized.Parameters(name = "fromCache:{0}")
    public static Iterable<Object[]> parameters() {
        return Arrays.asList(new Object[]{Boolean.TRUE}, new Object[]{Boolean.FALSE});
    }

    HazelcastInstance server;
    JavaSparkContext sparkContext;

    @Before
    public void setUp() throws Exception {
        System.setProperty("hazelcast.test.use.network", "true");
        System.setProperty("hazelcast.local.localAddress", "127.0.0.1");
        server = createHazelcastInstance();

        SparkConf conf = new SparkConf().setMaster("local[8]").setAppName(this.getClass().getName())
                .set("hazelcast.server.address", "127.0.0.1:5701")
                .set("spark.driver.host", "127.0.0.1");
        sparkContext = new JavaSparkContext(conf);
    }

    @After
    public void tearDown() throws Exception {
        server.getLifecycleService().terminate();
        sparkContext.stop();
    }

    @Test
    public void count() {
        HazelcastJavaRDD<Integer, Integer> hazelcastRDD = getPrepopulatedRDD();
        assertEquals(100, hazelcastRDD.count());
    }

    @Test
    public void isEmpty() {
        HazelcastJavaRDD<Integer, Integer> hazelcastRDD = getPrepopulatedRDD();
        assertFalse(hazelcastRDD.isEmpty());
    }

    @Test
    public void sortByKey() {
        HazelcastJavaRDD<Integer, Integer> hazelcastRDD = getPrepopulatedRDD();
        Tuple2<Integer, Integer> first = hazelcastRDD.sortByKey().first();
        assertEquals(0, first._1().intValue());
    }

    @Test
    public void countByKey() {
        HazelcastJavaRDD<Integer, Integer> hazelcastRDD = getPrepopulatedRDD();
        Map<Integer, Object> map = hazelcastRDD.countByKey();
        for (Object count : map.values()) {
            assertEquals(1L, count);
        }
    }

    @Test
    public void countByValue() {
        HazelcastJavaRDD<Integer, Integer> hazelcastRDD = getPrepopulatedRDD();
        Map<Tuple2<Integer, Integer>, Long> map = hazelcastRDD.countByValue();
        for (Long count : map.values()) {
            assertEquals(1, count.longValue());
        }
    }

    @Test
    public void filter() {
        HazelcastJavaRDD<Integer, Integer> hazelcastRDD = getPrepopulatedRDD();
        JavaPairRDD<Integer, Integer> filter = hazelcastRDD.filter(new LessThan10Filter());
        for (Integer value : filter.values().collect()) {
            assertTrue(value < 10);
        }
    }

    @Test
    public void min() {
        HazelcastJavaRDD<Integer, Integer> hazelcastRDD = getPrepopulatedRDD();
        Tuple2<Integer, Integer> min = hazelcastRDD.min(new ValueComparator());
        assertEquals(0, min._1().intValue());
        assertEquals(0, min._2().intValue());
    }

    @Test
    public void max() {
        HazelcastJavaRDD<Integer, Integer> hazelcastRDD = getPrepopulatedRDD();
        Tuple2<Integer, Integer> max = hazelcastRDD.max(new ValueComparator());
        assertEquals(99, max._1().intValue());
        assertEquals(99, max._2().intValue());
    }

    @Test
    public void flatMap() {
        HazelcastJavaRDD<Integer, Integer> hazelcastRDD = getPrepopulatedRDD();
        List<Object> values = hazelcastRDD.flatMap(new FlatMapValues()).collect();
        assertEquals(100, values.size());
    }

    private HazelcastJavaRDD<Integer, Integer> getPrepopulatedRDD() {
        HazelcastSparkContext hazelcastSparkContext = new HazelcastSparkContext(sparkContext);
        String name = randomName();
        if (fromCache) {
            CacheProxy<Integer, Integer> cacheProxy = HazelcastHelper.getServerCacheProxy(name, server);
            for (int i = 0; i < 100; i++) {
                cacheProxy.put(i, i);
            }
            return hazelcastSparkContext.fromHazelcastCache(name);
        } else {
            IMap<Integer, Integer> map = server.getMap(name);
            for (int i = 0; i < 100; i++) {
                map.put(i, i);
            }
            return hazelcastSparkContext.fromHazelcastMap(name);
        }
    }

    private static class FlatMapValues implements FlatMapFunction<Tuple2<Integer, Integer>, Object>, Serializable {
        @Override
        public Iterable<Object> call(Tuple2<Integer, Integer> integerIntegerTuple2) throws Exception {
            return Arrays.asList(integerIntegerTuple2._2());
        }
    }

    private static class LessThan10Filter implements Function<Tuple2<Integer, Integer>, Boolean>, Serializable {
        @Override
        public Boolean call(Tuple2<Integer, Integer> v1) throws Exception {
            return v1._2() < 10;
        }
    }

    private static class ValueComparator implements Comparator<Tuple2<Integer, Integer>>, Serializable {
        @Override
        public int compare(Tuple2<Integer, Integer> o1, Tuple2<Integer, Integer> o2) {
            if (o1._2() < o2._2()) {
                return -1;
            } else if (Objects.equals(o1._2(), o2._2())) {
                return 0;
            } else {
                return 1;
            }
        }
    }
}
