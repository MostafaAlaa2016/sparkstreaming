package com.etisalat.testing

/*
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Encoders
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
*/


import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

object STREAMING_MAIN {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("SIM_SWAP")
      .config("spark.master", "local")
      .config("redis.host", "px2661")
      .config("redis.port", "6379")
      .getOrCreate()
    val batchIntervalSeconds = 1

    val conf = new SparkConf().setMaster("local[*]")
      .setAppName("SIM_SWAP_BINS")
      .set("redis.host", "px2661")
      .set("redis.port", "6379")

    import org.apache.log4j.{Level, Logger}

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    val input_path2 = "file:///M:/Work/Etisalat/test"

    val input_path = "file:///C:/Users/mohamedamostafa/"

    val output_path =
      """file:///C:/Users/mohamedamostafa/"

    //val output_path = "file:///M://Learning//Master//CombinedWorkspace//Scala//STREAMING_BINS_SPARK" +
    //"//STREAMING_BINS_SPARK//out//soh//data1"

    def prepare[T: ClassTag](rdd: RDD[T], n: Int) =
      rdd.zipWithIndex.sortBy(_._2, true, n).keys

    def customZip[T: ClassTag, U: ClassTag](rdd1: RDD[T], rdd2: RDD[U]) = {
      val n = rdd1.partitions.size + rdd2.partitions.size
      prepare(rdd1, n).zip(prepare(rdd2, n))
    }

    def zipFunc(l: Iterator[String], r: Iterator[String]): Iterator[(String, String)] = {
      val res = new ListBuffer[(String, String)]
      // exercise for the reader: suck either l or r into a container before iterating
      // and consume it in random order to achieve more random pairing if desired
      //val r2 = r.next()
      while (l.hasNext && r.hasNext) {
        res += ((l.next(), r.next()))
      }
      res.iterator
    }


    val ssc = new StreamingContext(spark.sparkContext, Seconds(3))

    val CBCM_SOH_Input: DStream[String] = ssc.textFileStream(input_path)


    CBCM_SOH_Input.foreachRDD((rdd, time) => {

      println("===========New================")
      rdd.collect().foreach(println)
      println("=======================================")


      //first challenge how can I pass the account_number

      val inputDF: RDD[String] = rdd.map(_.split('|')).map(x => Seq(x(0), x(2), x(4))).flatMap(x => x).coalesce(1)
      val inputDF2: RDD[(String, String, String)] = rdd.map(_.split('|')).map(x => (x(0), x(2), x(4)))


      val inputDFCount = inputDF.count()

      if (inputDFCount > 0) {
        var columnsKeysSeq = List[String]()
        val DFCount: Int = inputDFCount.toInt / 3

        for (a <- 1 to DFCount) {
          columnsKeysSeq ::= "last_sim_swap"
          columnsKeysSeq ::= "portout_flag"
          columnsKeysSeq ::= "batch_id"
        }


        val columns_names: RDD[String] = spark.sparkContext.parallelize(columnsKeysSeq)
        val pairs: RDD[(String, String)] = inputDF.zipPartitions(columns_names, true)(zipFunc).map(x => (x._2, x._1))
        //pairs.foreach(println)


        val input_data = inputDF.collect().toSeq
        val zippedSeq = columnsKeysSeq.zip(input_data)
        //zippedSeq.foreach(println)

        //val columns_names2: RDD[String] = spark.sparkContext.parallelize(sample)

        var Sample = Seq("last_sim_swap","portout_flag","batch_id")

        inputDF2.foreach {
          case (row) => {
            val ACCOUNT_NUMBER = row._3
            println("Account = " + ACCOUNT_NUMBER)

            val inputRow: Seq[String] = Seq(row._1, row._2, row._3)
            //val zippedSeq: Seq[(String, String)] = columnsKeysSeq.zip(inputRow)
            val zippedSeq: Seq[(String, String)] = Sample.zip(inputRow)

            val flattedDF = spark.sparkContext.parallelize(zippedSeq)

            flattedDF.collect().foreach(println)
            /*val zippedDF = flattedDF.zipPartitions(columns_names2, true)(zipFunc) //.map(x => (x._2, x._1))
            zippedDF.collect().foreach(println)*/
          }
        }
        //spark.sparkContext.toRedisHASH(pairs, "Account:011")



      }

    }
    )

    ssc.start()
    ssc.awaitTermination()

  }

}

