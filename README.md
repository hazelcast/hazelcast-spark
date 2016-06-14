# Spark Connector for Hazelcast
Spark Connector for Hazelcast allows your Spark applications to connect Hazelcast cluster with the Spark RDD API.

## Features
- Read/Write support for Hazelcast Maps
- Read/Write support for Hazelcast Caches

## Configuration

### Properties
You can set the options below for the `SparkConf` object:

Property name                                  | Description                                       | Default value      
-----------------------------------------------|---------------------------------------------------|--------------------
hazelcast.server.addresses                     | Comma separated list of hazelcast server addresses  | 127.0.0.1:5701    
hazelcast.server.group.name                    | Group name of the Hazelcast Cluster | dev    
hazelcast.server.group.pass                    | Group password of the Hazelcast Cluster | dev-pass
hazelcast.spark.valueBatchingEnabled           | If enabled, retrieves values from hazelcast in batches for better performance, if disabled, for each key connector will make a retrieve call to the cluster for retrieving the most recent value. | true   
hazelcast.spark.readBatchSize                  | Number of entries to read in for each batch | 1000    
hazelcast.spark.writeBatchSize                 | Number of entries to write in for each batch | 1000    
hazelcast.spark.clientXmlPath                  | Location of the Hazelcast Client XML configuration file. | N/A    

### Creating the SparkContext

Scala :

```scala
val conf = new SparkConf()
          .set("hazelcast.server.addresses", "127.0.0.1:5701")
          .set("hazelcast.server.group.name", "dev")
          .set("hazelcast.server.group.pass", "dev-pass")
          .set("hazelcast.spark.valueBatchingEnabled", "true")
          .set("hazelcast.spark.readBatchSize", "5000")
          .set("hazelcast.spark.writeBatchSize", "5000")

val sc = new SparkContext("spark://127.0.0.1:7077", "appname", conf)
```
Java :
```java

SparkConf conf = new SparkConf()
          .set("hazelcast.server.addresses", "127.0.0.1:5701")
          .set("hazelcast.server.group.name", "dev")
          .set("hazelcast.server.group.pass", "dev-pass")
          .set("hazelcast.spark.valueBatchingEnabled", "true")
          .set("hazelcast.spark.readBatchSize", "5000")
          .set("hazelcast.spark.writeBatchSize", "5000")

JavaSparkContext jsc = new JavaSparkContext("spark://127.0.0.1:7077", "appname", conf);
// wrapper to provide Hazelcast related functions to the Spark Context.
HazelcastSparkContext hsc = new HazelcastSparkContext(jsc);
```



## Reading Data from Hazelcast

After SparkContext is created, we can load data stored in Hazelcast Maps and Caches into Spark as RDDs like below.

Scala :
```scala
import com.hazelcast.spark.connector.{toSparkContextFunctions}

// read from map
val rddFromMap = sc.fromHazelcastMap("map-name-to-be-loaded")

// read from cache
val rddFromCache = sc.fromHazelcastCache("cache-name-to-be-loaded")
```
Java :
```java
// read from map
HazelcastJavaRDD rddFromMap = hsc.fromHazelcastMap("map-name-to-be-loaded")

// read from cache
HazelcastJavaRDD rddFromCache = hsc.fromHazelcastCache("cache-name-to-be-loaded")

```

## Writing Data to Hazelcast

After any computation you can save your `PairRDD`s to Hazelcast Cluster as Maps or Caches.


Scala :

```scala
import com.hazelcast.spark.connector.{toHazelcastRDDFunctions}
val rdd: RDD[(Int, Long)] = sc.parallelize(1 to 1000).zipWithIndex()

// write to map
rdd.saveToHazelcastMap(name);

// write to cache
rdd.saveToHazelcastCache(name);
```
Java :
```java
import static com.hazelcast.spark.connector.HazelcastJavaPairRDDFunctions.javaPairRddFunctions;

JavaPairRDD<Object, Long> rdd = hsc.parallelize(new ArrayList<Object>() {{
    add(1);
    add(2);
    add(3);
}}).zipWithIndex();

// write to map
javaPairRddFunctions(rdd).saveToHazelcastMap(name);

// write to cache
javaPairRddFunctions(rdd).saveToHazelcastCache(name);

```

# Known Limitations

The data loaded from hazelcast maps and caches should be static, in other words, it shouldn't be mutated while being read by Apache Spark. Failing to do could end up missing or duplicate entries in the RDD.  
