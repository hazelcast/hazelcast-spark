

# Table of Contents
adfadfadf
* [Spark Connector for Hazelcast](#spark-connector-for-hazelcast-beta)
* [Features](#features)
* [Requirements](#requirements)
* [Releases](#releases)
  * [Stable](#stable)
  * [Snapshots](#snapshots)
* [Configuration](#configuration)
  * [Properties](#properties)
  * [Creating the SparkContext](#creating-the-sparkcontext)
* [Reading Data from Hazelcast](#reading-data-from-hazelcast)
* [Writing Data to Hazelcast](#writing-data-to-hazelcast)
* [Code Samples](#code-samples)
* [Testing](#testing)
* [Known Limitations](#known-limitations)


# Spark Connector for Hazelcast

Spark Connector for Hazelcast allows your Spark applications to connect to a Hazelcast cluster with the Spark RDD API.

# Related Project - Hazelcast Jet
Spark integration is one of several Hazelcast Big Data projects. We also offer a High Performance Stream Processing Engine, [Hazelcast Jet](https://github.com/hazelcast/hazelcast-jet).

# Features

- Read/Write support for Hazelcast Maps
- Read/Write support for Hazelcast Caches

# Requirements

- Hazelcast 3.7.x or higher
- Apache Spark 1.6.1 
- Apache Spark 2.1.0 or higher

# Releases

SBT (Scala Build Tool) and Maven dependencies for Spark Connector's stable and snapshot releases are shown in the following sections.

## Stable

**SBT:**

```scala
libraryDependencies += "com.hazelcast" % "hazelcast-spark" % "0.1"
```


**Maven:**

```xml
<dependency>
    <groupId>com.hazelcast</groupId>
    <artifactId>hazelcast-spark</artifactId>
    <version>0.1</version>
</dependency>
```

## Snapshots

**SBT:**

Add Sonatype resolver to the SBT as shown below:

```scala
resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
```

**Maven:**

Add Sonatype repository to your `pom.xml` as shown below:

```xml
<repository>
   <id>sonatype-snapshots</id>
   <name>Sonatype Snapshot Repository</name>
   <url>https://oss.sonatype.org/content/repositories/snapshots</url>
   <releases>
       <enabled>false</enabled>
   </releases>
   <snapshots>
       <enabled>true</enabled>
   </snapshots>
</repository>
```


# Configuration

Spark Connector uses Hazelcast Client to talk with a Hazelcast Cluster. You can provide the configuration details of the client to be able to connect to a Hazelcast Cluster. If you have a complex setup, you can also provide a fully configured Hazelcast Client configuration XML to configure the Hazelcast Client.

## Properties

You can set the options below for the `SparkConf` object:

Property name                                  | Description                                       | Default value      
-----------------------------------------------|---------------------------------------------------|--------------------
hazelcast.server.addresses                     | Comma separated list of Hazelcast server addresses.  | 127.0.0.1:5701    
hazelcast.server.groupName                    | Group name of the Hazelcast Cluster. | dev    
hazelcast.server.groupPass                    | Group password of the Hazelcast Cluster. | dev-pass
hazelcast.spark.valueBatchingEnabled           | If enabled, retrieves values from Hazelcast in batches for better performance. If disabled, for each key, the connector will make a retrieve call to the cluster for retrieving the most recent value. | true   
hazelcast.spark.readBatchSize                  | Number of entries to read in for each batch. | 1000    
hazelcast.spark.writeBatchSize                 | Number of entries to write in for each batch. | 1000    
hazelcast.spark.clientXmlPath                  | Location of the Hazelcast Client XML configuration file. | N/A    

## Creating the SparkContext

**Scala:**

```scala
val conf = new SparkConf()
          .set("hazelcast.server.addresses", "127.0.0.1:5701")
          .set("hazelcast.server.groupName", "dev")
          .set("hazelcast.server.groupPass", "dev-pass")
          .set("hazelcast.spark.valueBatchingEnabled", "true")
          .set("hazelcast.spark.readBatchSize", "5000")
          .set("hazelcast.spark.writeBatchSize", "5000")

val sc = new SparkContext("spark://127.0.0.1:7077", "appname", conf)
```

**Java:**

```java

SparkConf conf = new SparkConf()
          .set("hazelcast.server.addresses", "127.0.0.1:5701")
          .set("hazelcast.server.groupName", "dev")
          .set("hazelcast.server.groupPass", "dev-pass")
          .set("hazelcast.spark.valueBatchingEnabled", "true")
          .set("hazelcast.spark.readBatchSize", "5000")
          .set("hazelcast.spark.writeBatchSize", "5000")

JavaSparkContext jsc = new JavaSparkContext("spark://127.0.0.1:7077", "appname", conf);
// wrapper to provide Hazelcast related functions to the Spark Context.
HazelcastSparkContext hsc = new HazelcastSparkContext(jsc);
```



# Reading Data from Hazelcast

After `SparkContext` is created, you can load data stored in Hazelcast Maps and Caches into Spark as RDDs as shown below:

**Scala:**

```scala
import com.hazelcast.spark.connector.{toSparkContextFunctions}

// read from map
val rddFromMap = sc.fromHazelcastMap("map-name-to-be-loaded")

// read from cache
val rddFromCache = sc.fromHazelcastCache("cache-name-to-be-loaded")
```

**Java:**

```java
// read from map
HazelcastJavaRDD rddFromMap = hsc.fromHazelcastMap("map-name-to-be-loaded")

// read from cache
HazelcastJavaRDD rddFromCache = hsc.fromHazelcastCache("cache-name-to-be-loaded")
```

# Writing Data to Hazelcast

After any computation, you can save your `PairRDD`s to Hazelcast Cluster as Maps or Caches as shown below:


**Scala:**

```scala
import com.hazelcast.spark.connector.{toHazelcastRDDFunctions}
val rdd: RDD[(Int, Long)] = sc.parallelize(1 to 1000).zipWithIndex()

// write to map
rdd.saveToHazelcastMap(name);

// write to cache
rdd.saveToHazelcastCache(name);
```

**Java:**

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

# Code Samples

You can find the code samples for Hazelcast Spark Connector at https://github.com/hazelcast/hazelcast-code-samples/tree/master/hazelcast-integration/spark.


# Testing

Run `./sbt clean test` command to execute the test suite.

# Known Limitations

If Hazelcast's data structure is modified (keys inserted or deleted) while Apache Spark is iterating over it, the RDD may encounter the same entry several times and fail to encounter other entries, even if they were present at the time of construction and untouched during the iteration. It is therefore recommended to keep the dataset stable while being read by Spark.
