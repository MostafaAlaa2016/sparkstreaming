package com.streaming.filesys

import com.redis.RedisClient
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


/**
  * @author Mostafa Alaa  2018-03-25
  * Application.scala Main class
  * @version 1.4
  * @see updateRedisStreaming,appendFile,listFilesInDir,readFiles,moveFiles,renameFile,handleOldFiles
  * @since 1.0
  * Scala code to read data from raw files for the source file system ,apply transformations as per given business logic and
  * load the transformed data into Redis DB
  * This program aimed to handle the streaming data for some Flags to capture the fraud cases
  *
  **/

object Application extends LazyLogging {
  def main(args: Array[String]): Unit = {

    if (args.length != 6) {
      System.err.println("You arguments were " + args.mkString("[", ", ", "]"))
      System.err.println(
        """
          |Usage: com.streaming.filesys <inputPath> <streamingLatency> <targetTable>   <checkPointPath>.
          |     <inputPath> input path that Spark Streaming would read it to receive data.
          |     <StreamingLatency> streaming delay.
          |     <targetTable> Redis key schema name .
          |     <checkpoint-directory> directory to HDFS-compatible file system which checkpoint
          |     <backlogfilePath> backlog files path  .
          |     <archivePath> Archive  path that Spark Streaming would move it to .
          |
          |In local mode, <master> should be 'local[n]' with n > 1
          |<checkpoint-directory> must be absolute paths
        """.stripMargin
      )
      System.exit(1)
    }

  /*Below functions is replaced by Shell scripts*/
/*    val oldBackLogFiles: List[String] = utils.readFiles(args(4))
    if(!oldBackLogFiles.isEmpty)
      {
        oldBackLogFiles.foreach { file =>
          logger.info("Start clear the processed files")
          utils.moveFiles(file, file.replace("RAW_FILES","RAW_FILES_ARCHIVE"))
        }


        utils.appendFile(args(4), "",false)
        var newRun = true
      }*/

    val regExp: Regex = "\\b(hdfs:|file:)\\S+".r

    val spark: SparkSession = SparkSession
      .builder //.master("local")
      .appName("STREAMING::: FRAUD")
      .config("redis.host", "px2661")
      .config("redis.port", "6379")
      .getOrCreate()

    val sc = spark.sparkContext

    //for connection pool handling
    object RedisConnection extends Serializable {
      lazy val conn: RedisClient = new RedisClient("10.51.194.67", 6379)
    }

    val hadoopFS: FileSystem = FileSystem.get(sc.hadoopConfiguration)


    startSohStreaming(args)

    def startSohStreaming(param: Array[String]): Unit = {
      //val context: StreamingContext = StreamingContext.getActiveOrCreate(param(3), creatingFunc _)
      val context: StreamingContext = StreamingContext.getActiveOrCreate(param(3),
        () => {
          creatingFunc(param)
        })
      logger.info("Starting Streaming")
      context.start()
      context.awaitTermination()
    }

    /**
      * @author Mostafa Alaa Mohamedamostafa@etisalat.ae
      * @param param:
      * @since 1.0
      */
    def creatingFunc(param: Array[String]): StreamingContext = {
      val ssc = new StreamingContext(sc, Seconds(param(1).toLong)) // new context
      logger.info("Creating function called to create new StreamingContext")
      ssc.checkpoint(param(3))
      //ssc.sparkContext.hadoopConfiguration
        //.set("spark.streaming.fileStream.minRememberDuration","12000")
      //ssc.sparkContext.hadoopConfiguration.set("newFilesOnly ","false")
      //val CBCM_SOH_Input: DStream[String] = ssc.textFileStream(param(0))
      val CBCM_SOH_Input: DStream[String] = ssc.fileStream[LongWritable, Text, TextInputFormat](param(0),
        (p: Path) => {
          if (p.getName.toLowerCase.endsWith(".dsv")) true else false // scalastyle:ignore
        }, newFilesOnly = false).map(_._2.toString)

      processSOHInputFiles(CBCM_SOH_Input, param(2), param(4), param(5))

      ssc
    }

    /**
      * @author Mostafa Alaa Mohamedamostafa@etisalat.ae
      * @param ds:
      * @param targetTable:
      * @param archivePath:
      * @since 1.0
      */
    def processSOHInputFiles(ds: DStream[String], targetTable: String, backlogfilePath: String,archivePath: String): Unit = {
      var files = new ListBuffer[String]()

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
          val requiredSubRequests = Set("70", "1", "1106", "1109","1110","1086", "1087","1088","1107","1108")
          //val requiredSubRequests = Set("70","1")
          val inputDF: RDD[Array[String]] = rdd.map(_.split('|')).filter(x => x.length == 6) // make sure the number of
          // columns is 6

          val inputDfFiltered = inputDF.filter(x=> requiredSubRequests.contains(x(2))) // check for sub request
          inputDfFiltered.cache()
          //val inputDfFiltered = inputDF.filter(x=> x(2) =="70" || x(2)=="1")
          logger.debug("******** inputDfFiltered = " + inputDfFiltered.count())
          //inputDfFiltered.map(x => x(2)).take(1000).foreach(println)
          if(!inputDfFiltered.isEmpty())
            {
              inputDF.foreach(
                row => {

                  logger.info("Processing File(s):")
                  logger.info("ACCOUNT_NUMBER = " + row(4)) //ACCOUNT_NUMBER
                  logger.info("SUB_REQ = " + row(2)) //SUB_REQ
                  logger.info("Timestamp = " + row(0))//Timestamp

                  val inputRows: Map[String, String] = Map("ACCOUNT_NUMBER" -> row(4).replace("\"", ""),//remove "
                    "SUB_REQ" -> row(2).toString,
                    "Timestamp" -> row(0).replace("\"", ""))

                  utils.updateRedisStreaming(inputRows, targetTable)



                })

            }else{
            logger.warn("###### Totally Malformed file File ######")
            logger.warn("lines" + rdd.collect().foreach(println))
          }



        }
        else {
          logger.warn("###### Empty File ######")
        }
        if(!files.isEmpty){

          files.foreach((name) => {
            logger.info("Looping for File(s):")
            logger.info(name.toString)
            val sourceFilePath= name.toString.replace("hdfs://prd", "/bigpprd")
            //val sourceFilePath= name.toString.replace("hdfs://aubinsnp", "/rtmstaging")
            //aubinsnp means ==> au (abu dhabi), bins (big insight),np(non-production)

            logger.info(sourceFilePath)

            utils.appendFile(backlogfilePath,sourceFilePath, appendFlag= true)

            val archiveFilePath = archivePath+"/"+name.toString.split("/")(7)

            logger.info(archiveFilePath)


            utils.moveFiles(sourceFilePath, archiveFilePath)
          })
          files.clear()
          files = new ListBuffer[String]()
        }

      })



    }
  }
}
