package com.hazelcast.spark.connector.util

import com.hazelcast.spark.connector.util.ConnectionUtil.closeAll
import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd, SparkListenerJobStart}

object CleanupUtil {

  val jobIds: collection.mutable.Map[Int, Seq[Int]] = collection.mutable.Map[Int, Seq[Int]]()
  val cleanupJobRddName: String = "HazelcastResourceCleanupJob"

  def addCleanupListener(sc: SparkContext): Unit = {
    sc.addSparkListener(new SparkListener {
      override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
        this.synchronized {
          jobStart.stageInfos.foreach(info => {
            info.rddInfos.foreach(rdd => {
              if (!cleanupJobRddName.equals(rdd.name)) {
                val ids: Seq[Int] = info.rddInfos.map(_.id)
                val maybeIds: Option[Seq[Int]] = jobIds.get(jobStart.jobId)
                if (maybeIds.isDefined) {
                  jobIds.put(jobStart.jobId, ids ++ maybeIds.get)
                } else {
                  jobIds.put(jobStart.jobId, ids)
                }
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
              val rddId: Option[Seq[Int]] = jobIds.get(jobEnd.jobId)
              if (rddId.isDefined) {
                sc.parallelize(1 to workers, workers).setName(cleanupJobRddName).foreachPartition(it ⇒ closeAll(rddId.get))
              }
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
