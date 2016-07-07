package com.hazelcast.spark.connector;

import com.hazelcast.spark.connector.rdd.HazelcastRDDFunctions;
import org.apache.spark.api.java.JavaPairRDD;

/**
 * Acts as static factory for {@link HazelcastRDDFunctions} which provides Hazelcast Spark Connector functionality.
 */
public final class HazelcastJavaPairRDDFunctions {

    private HazelcastJavaPairRDDFunctions() {
    }

    public static <K, V> HazelcastRDDFunctions javaPairRddFunctions(JavaPairRDD<K, V> rdd) {
        return new HazelcastRDDFunctions<>(rdd.rdd());
    }

}
