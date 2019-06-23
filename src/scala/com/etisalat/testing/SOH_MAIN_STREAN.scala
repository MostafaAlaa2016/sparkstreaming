package com.etisalat.testing

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.StreamingContext


object SOH_MAIN_STREAN extends LazyLogging {
  def main(args: Array[String]): Unit = {


   // val inputPath = args(0)
    //val streamingLatendy = args(1)
    //val targetTable = args(2)
    //val checkPointPath = args(3)
    //val checkPointLatendy = args(4)

    val inputPath = "file:///C:/Users/mohamedamostafa/"
    val streamingLatendy = 10
    val targetTable = "ACCOUNT:"

    val checkPointPath = "file:///C:/Users/mohamedamostafa/"


    val stopActiveContext = true
    var isStopped = false


/*    val spark = SparkSession
      .builder.master("local")
      .appName("STREAMING::: SIM_SWAP_FRAUD")
      .config("redis.host", "dx2661")
      .config("redis.port", "6379")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .getOrCreate()*/


    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)


    if (stopActiveContext) {
      StreamingContext.getActive.foreach {
        _.stop(stopSparkContext = false)
      }
    }
    /*def creatingFunc(): StreamingContext = {
      val ssc = new StreamingContext(spark.sparkContext, Seconds(streamingLatendy.toLong)) // new context
      //ssc.checkpoint(checkPointPath)
      val CBCM_SOH_Input: DStream[String] = ssc.textFileStream(inputPath)
      updateRedisStreaming(CBCM_SOH_Input,targetTable)
      ssc.remember(Seconds(streamingLatendy.toLong))
      println("Creating function called to create new StreamingContext")
      newContextCreated = true
      ssc
    }*/
    //val context: StreamingContext = StreamingContext.getActiveOrCreate(checkPointPath, creatingFunc _)
    /*if (StreamingUtils.newContextCreated) {
      println("New context created from currently defined creating function")
    } else {
      println("Existing context running or recovered from checkpoint, may not be running currently defined creating function")
    }
*/

    //val CBCM_SOH_Input: DStream[String] = context.textFileStream(inputPath)
/*    StreamingUtils.updateRedisStreaming(CBCM_SOH_Input,targetTable)*/

    /*

    */


    //context.start()


 /*   while (!isStopped) {
      println("calling awaitTerminationOrTimeout")
      isStopped = context.awaitTerminationOrTimeout(streamingLatendy.toLong * 2 * 1000)
      if (isStopped)
        println("confirmed! The streaming context is stopped. Exiting application...")
      else
        println("Streaming App is still running. Timeout...")
      checkShutdownMarker(context,streamingLatendy.toLong)
      if (!isStopped && StreamingUtils.stopFlag) {
        println("stopping ssc right now")
        context.stop(true, true)
        println("ssc is stopped!!!!!!!")
      }
    }*/
    /*  def checkShutdownMarker(context: StreamingContext, streamingLatendy: Long) = {
        if (!stopFlag) {
          val fs = FileSystem.get(new Configuration())
          stopFlag = fs.exists(new Path(shutdownMarker))
        }
        context.awaitTerminationOrTimeout(streamingLatendy * 2 * 1000)

      }*/

  }


}
