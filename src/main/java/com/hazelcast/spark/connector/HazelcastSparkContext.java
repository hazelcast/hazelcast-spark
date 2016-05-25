package com.hazelcast.spark.connector;

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
        ClassTag<K> kt = HazelcastHelper.getClassTag();
        ClassTag<V> vt = HazelcastHelper.getClassTag();
        return new HazelcastJavaRDD<K, V>(hazelcastRDD, kt, vt);
    }

    public <K, V> HazelcastJavaRDD<K, V> fromHazelcastMap(String mapName) {
        HazelcastRDD<K, V> hazelcastRDD = hazelcastSparkContextFunctions.fromHazelcastMap(mapName);
        ClassTag<K> kt = HazelcastHelper.getClassTag();
        ClassTag<V> vt = HazelcastHelper.getClassTag();
        return new HazelcastJavaRDD<K, V>(hazelcastRDD, kt, vt);
    }
}
