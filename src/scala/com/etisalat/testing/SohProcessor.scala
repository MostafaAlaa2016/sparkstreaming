package com.etisalat.testing

import com.redis.RedisClient
import com.streaming.filesys.utils
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex

object SohProcessor extends LazyLogging with Serializable {


  val regExp: Regex = "\\b(hdfs:|file:)\\S+".r

  val spark: SparkSession = SparkSession
    .builder //.master("local")
    .appName("STREAMING::: FRAUD")
    .config("redis.host", "px2661")
    .config("redis.port", "6379")
    //.config("spark.streaming.stopGracefullyOnShutdown", "true")
    .getOrCreate()

  val sc = spark.sparkContext

  //for connection pool handling
  object RedisConnection extends Serializable {
    lazy val conn: RedisClient = new RedisClient("10.51.194.67", 6379)
  }

  val hadoopFS: FileSystem = FileSystem.get(sc.hadoopConfiguration)


  def creatingFunc(args: Array[String]): StreamingContext = {
    val ssc = new StreamingContext(sc, Seconds(args(1).toLong)) // new context
    logger.info("Creating function called to create new StreamingContext")
    ssc.checkpoint(args(4))
    //val CBCM_SOH_Input: DStream[String] = ssc.textFileStream(args(0))

    val CBCM_SOH_Input: DStream[String] = ssc.fileStream[LongWritable, Text, TextInputFormat](args(0),
      (p: Path) => {
        if (p.getName.toLowerCase.endsWith("dsv")) true else false // scalastyle:ignore
      }, true).map(_._2.toString)

    processSOHInputFiles(CBCM_SOH_Input, args(2), args(4))
    ssc.remember(Seconds(args(1).toLong))

    ssc
  }


  def processSOHInputFiles(ds: DStream[String], targetTable: String, archivePath: String): Unit = {
    var files = new ListBuffer[String]()
    //var outList = List(Nil)

    logger.info("looping Starting Moding")
    logger.debug("files Names " + files.foreach(println))

    ds.foreachRDD(foreachFunc = (rdd, time) => {

      logger.info("inside foreachRDD")
      if (!rdd.isEmpty()) {
        logger.debug("lines" + rdd.collect().foreach(println))
        files.clear()
        files = new ListBuffer[String]()

        logger.info(rdd.toDebugString)

        regExp.findAllMatchIn(rdd.toDebugString).foreach((name) => {
          logger.info("Adding File(s) to List:")
          logger.info(name.toString)
          files += name.toString
        })

        val inputDF: RDD[Array[String]] = rdd.map(_.split('|')).coalesce(1)

        inputDF.foreach(
          row => {
            logger.info("Processing File(s):")
            logger.info("ACCOUNT_NUMBER = " + row(4))
            logger.info("SUB_REQ = " + row(2))
            logger.info("Timestamp = " + row(0))

            val inputRows: Map[String, String] = Map("ACCOUNT_NUMBER" -> row(2).replace("\"", ""),
              "SUB_REQ" -> row(1),
              "Timestamp" -> row(0).replace("\"", ""))

            utils.updateRedisStreaming(inputRows, targetTable)
            //val filesList = files.toList
            files.foreach((name) => {
              logger.info("Looping for File(s):")
              logger.info(name.toString)
              utils.appendFile(archivePath, name.toString,true)

            })
          })
      }
      else {
        logger.warn("###### Empty File ######")
      }
    })

  }


}
