package com.hazelcast.spark.connector.util

import com.hazelcast.spark.connector.util.ConnectionUtil.closeAll
import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd, SparkListenerJobStart}

object CleanupUtil {

  val jobIds: collection.mutable.Set[Int] = collection.mutable.Set[Int]()
  val cleanupJobRddName: String = "HazelcastResourceCleanupJob"

  def addCleanupListener(sc: SparkContext): Unit = {
    sc.addSparkListener(new SparkListener {
      override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
        this.synchronized {
          jobStart.stageInfos.foreach(info => {
            info.rddInfos.foreach(rdd => {
              if (!cleanupJobRddName.equals(rdd.name)) {
                jobIds += jobStart.jobId
              }
            })
          })
        }
      }

      override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
        this.synchronized {
          if (jobIds.contains(jobEnd.jobId)) {
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
