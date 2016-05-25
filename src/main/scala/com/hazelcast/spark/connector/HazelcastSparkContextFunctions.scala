package com.hazelcast.spark.connector

import org.apache.spark.SparkContext
import org.apache.spark.scheduler._

import scala.util.Try

class HazelcastSparkContextFunctions(@transient val sc: SparkContext) extends Serializable {

  val jobIds: collection.mutable.Set[Int] = collection.mutable.Set[Int]()
  val cleanupJobRddName: String = "HazelcastResourceCleanupJob"

  def fromHazelcastCache[K, V](cacheName: String): HazelcastRDD[K, V] = {
    addCleanupListener()
    new HazelcastRDD[K, V](sc, cacheName, true, getServerAddress, isBatchingEnabled)
  }

  def fromHazelcastMap[K, V](mapName: String): HazelcastRDD[K, V] = {
    addCleanupListener()
    new HazelcastRDD[K, V](sc, mapName, false, getServerAddress, isBatchingEnabled)
  }

  private def isBatchingEnabled: Boolean = {
    Try(sc.getConf.get("hazelcast.batch.values").toBoolean).getOrElse(true)
  }

  private def getServerAddress: String = {
    sc.getConf.get("hazelcast.server.address")
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
              sc.parallelize(1 to workers, workers).setName(cleanupJobRddName).foreachPartition(it ⇒ ConnectionManager.closeAll())
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
