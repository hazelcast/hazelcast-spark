package com.hazelcast.spark.connector

import com.hazelcast.spark.connector.ConnectionManager.closeAll
import com.hazelcast.spark.connector.Properties.{getServerAddress, isBatchingEnabled}
import org.apache.spark.SparkContext
import org.apache.spark.scheduler._

class HazelcastSparkContextFunctions(@transient val sc: SparkContext) extends Serializable {

  val jobIds: collection.mutable.Set[Int] = collection.mutable.Set[Int]()
  val cleanupJobRddName: String = "HazelcastResourceCleanupJob"

  def fromHazelcastCache[K, V](cacheName: String): HazelcastRDD[K, V] = {
    addCleanupListener()
    new HazelcastRDD[K, V](sc, cacheName, true, getServerAddress(sc), isBatchingEnabled(sc))
  }

  def fromHazelcastMap[K, V](mapName: String): HazelcastRDD[K, V] = {
    addCleanupListener()
    new HazelcastRDD[K, V](sc, mapName, false, getServerAddress(sc), isBatchingEnabled(sc))
  }

  private def addCleanupListener(): Unit = {
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
            try {
              val workers = sc.getConf.getInt("spark.executor.instances", sc.getExecutorStorageStatus.length)
              sc.parallelize(1 to workers, workers).setName(cleanupJobRddName).foreachPartition(it ⇒ closeAll())
              jobIds -= jobEnd.jobId
            } catch {
              case e: Exception =>
            }
          }
        }
      }
    })
  }
}
