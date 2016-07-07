package com.hazelcast.spark.connector;

import com.hazelcast.spark.connector.rdd.HazelcastJavaRDD;
import com.hazelcast.spark.connector.rdd.HazelcastRDD;
import com.hazelcast.spark.connector.util.HazelcastUtil;
import org.apache.spark.api.java.JavaSparkContext;
import scala.reflect.ClassTag;

/**
 * Wrapper over {@link JavaSparkContext} to bring Hazelcast related functionality to the Spark Context.
 */
public class HazelcastSparkContext {

    private final JavaSparkContext jsc;
    private final HazelcastSparkContextFunctions hazelcastSparkContextFunctions;

    public HazelcastSparkContext(JavaSparkContext jsc) {
        this.jsc = jsc;
        this.hazelcastSparkContextFunctions = new HazelcastSparkContextFunctions(jsc.sc());
    }

    public <K, V> HazelcastJavaRDD<K, V> fromHazelcastCache(String cacheName) {
        HazelcastRDD<K, V> hazelcastRDD = hazelcastSparkContextFunctions.fromHazelcastCache(cacheName);
        ClassTag<K> kt = HazelcastUtil.getClassTag();
        ClassTag<V> vt = HazelcastUtil.getClassTag();
        return new HazelcastJavaRDD<>(hazelcastRDD, kt, vt);
    }

    public <K, V> HazelcastJavaRDD<K, V> fromHazelcastMap(String mapName) {
        HazelcastRDD<K, V> hazelcastRDD = hazelcastSparkContextFunctions.fromHazelcastMap(mapName);
        ClassTag<K> kt = HazelcastUtil.getClassTag();
        ClassTag<V> vt = HazelcastUtil.getClassTag();
        return new HazelcastJavaRDD<>(hazelcastRDD, kt, vt);
    }
}
