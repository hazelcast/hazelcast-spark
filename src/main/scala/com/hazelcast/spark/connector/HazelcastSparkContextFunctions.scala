package com.hazelcast.spark.connector

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{SparkListenerApplicationStart, _}

class HazelcastSparkContextFunctions(@transient val sc: SparkContext) extends Serializable {

  val jobIds: collection.mutable.Set[Int] = collection.mutable.Set[Int]()
  val cleanupJobRddName: String = "HazelcastResourceCleanupJob"

  def fromHazelcastCache[K, V](cacheName: String): HazelcastRDD[K, V] = {
    sc.addSparkListener(new SparkListener {
      override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
        jobStart.stageInfos.foreach(info => {
          info.rddInfos.foreach(rdd => {
            if (!cleanupJobRddName.equals(rdd.name)) {
              jobIds += jobStart.jobId
            }
          })
        })
      }

      override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
        if (jobIds.contains(jobEnd.jobId)) {
          if (!sc.isStopped) {
            val workers = sc.getConf.getInt("spark.executor.instances", sc.getExecutorStorageStatus.length)
            sc.parallelize(1 to workers, workers).setName(cleanupJobRddName).foreachPartition(it ⇒ ConnectionManager.closeAll())
            jobIds -= jobEnd.jobId
          }
        }
      }
    })
    val server: String = sc.getConf.get("hazelcast.server.address")
    new HazelcastRDD[K, V](sc, cacheName, server)
  }
}
