import com.hazelcast.spark.connector.HazelcastRDD;
import org.apache.spark.SparkContext;

public class HazelcastSparkContext {

    final SparkContext sc;

    public HazelcastSparkContext(SparkContext sc) {
        this.sc = sc;
    }

    public <K, V> HazelcastRDD<K, V> fromHazelcastCache(String cacheName) {
        return new HazelcastRDD<K, V>(sc, cacheName, "");
    }
}
