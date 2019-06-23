package com.streaming.filesys

import java.io.{FileNotFoundException, FileWriter, IOException}
import java.text.SimpleDateFormat
import java.util.Calendar

import scala.io.Source
import com.etisalat.testing.SohProcessor.RedisConnection
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.hadoop.fs.FileSystem
import java.nio.file.{Files, StandardCopyOption}
import java.io.File
import util.Try


object utils extends LazyLogging {


  val dataTimeFormat = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss")
  val inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")




  val batchIdFormat = new SimpleDateFormat("yyyyMMddhhmmss")


  /**
    * @author Mostafa Alaa 
    * @param row         :
    * @param targetTable :
    * @since 1.0
    */
  def updateRedisStreaming(row: Map[String, String], targetTable: String) = row("SUB_REQ") match {
    case x if (x == "70" || x == "1") =>
      /* SIMSWAP SUBREQ*/
      logger.info("SIMSWAP SUBREQ")
      logger.info(
                   "dataTimeFormat transformed = " + dataTimeFormat
                     .format(inputFormat.parse(row("Timestamp"))))
      RedisConnection.conn.hmset(
                                  targetTable + row("ACCOUNT_NUMBER"),
                                  Map(
                                       "last_sim_swap" -> dataTimeFormat.format(inputFormat.parse(row("Timestamp"))),
                                    "portout_flag" -> "F", "batch_id" -> batchIdFormat.format(Calendar.getInstance().getTime)
                                  ))
    case x if (x == "1106" || x == "1109" || x == "1110") =>
      /* SIMSWAP SUBREQ*/
      logger.info("SIMSWAP SUBREQ")
      RedisConnection.conn.hmset(
                                  targetTable + row("ACCOUNT_NUMBER"),
                                  Map(
                                       "last_sim_swap" ->
                                         dataTimeFormat.format(inputFormat.parse(row("Timestamp"))),
                                       "portout_flag" -> "F",
                                       "batch_id" -> batchIdFormat
                                         .format(Calendar.getInstance().getTime))
                                )

    case x if (x == "1086" || x == "1087" || x == "1088" || x == "1107" || x == "1108") =>
      logger.info("PORTOUT SUBREQ")
      RedisConnection.conn.hmset(
                                  targetTable + row("ACCOUNT_NUMBER"),
                                  Map(
                                       "portout_flag" -> "T",
                                       "batch_id" -> batchIdFormat
                                         .format(Calendar.getInstance().getTime))
                                )


    case _ =>
      logger.warn("Not Recognize SUB_REQUEST")

  }

  /**
    * @author Mostafa Alaa 
    * @param fileName   :
    * @param line       :
    * @param appendFlag :
    * @since 1.0
    */
  def appendFile(fileName: String, line: String, appendFlag: Boolean): Unit = {

    val fw = new FileWriter(fileName, appendFlag)
    try {
      fw.write(line)
      fw.write("\n")
    }
    finally fw.close()

  }

  /**
    * @author Mostafa Alaa 
    * @param dir :
    * @since 1.0
    */
  def listFilesInDir(dir: String): List[File] = {

    val folderName = new File(dir)
    folderName.listFiles.filter(_.isFile).toList

  }

  /**
    * @author Mostafa Alaa 
    * @param fileName :
    * @since 1.0
    */
  def readFiles(fileName: String): List[String] = {

    try {
      logger.info("Start reading files from processed file path  " + fileName)
      Source.fromFile(fileName).getLines.toList
    }
    catch {
      case ioe: FileNotFoundException => {
        logger.error("Couldn't find file " + fileName)
        Nil
      }
      case io: IOException => {
        logger.error("Had an IOException trying to read that file" + io.printStackTrace())
        Nil
      }
      case e: Exception => {
        logger.error(e.getMessage)
        Nil
      }
      case t: Throwable => {
        logger.error("Throwable " + t.printStackTrace())
        Nil
      }
    }
  }


  /**
    * @author Mostafa Alaa 
    * @param inputFile :
    * @since 1.0
    */
  def moveFiles(inputFile: String, archivePath: String) = {
    logger.info("Start move processed file path  " + inputFile)
    val d1 = new File(inputFile).toPath
    val d2 = new File(archivePath).toPath

    try {
      Files.move(d1, d2, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
    }
    catch {
      case ioe: FileNotFoundException => {
        logger.error("Couldn't find file " + inputFile)
      }
      case io: IOException => {
        logger.error("Had an IOException trying to read that file" + io.printStackTrace())
      }
      case e: Exception => {
        logger.error(e.getMessage)
      }
      case t: Throwable => {
        logger.error("Throwable " + t.printStackTrace())
      }
    }

  }

  /**
    * @author Mostafa Alaa 
    * @param oldName :
    * @param newName :
    * @since 1.0
    */
  def mv(oldName: String, newName: String) = {
    Try(new File(oldName).renameTo(new File(newName))).getOrElse(false)
  }


  /**
    * @author Mostafa Alaa 
    * @param inputFilePathName :
    * @param outputFile        :
    * @param hadoopFS          :
    * @since 1.0
    */
  def renameFile(inputFilePathName: String, outputFile: String, hadoopFS: FileSystem): Unit = {
    try {
      println("renameFile Function")
      val inputFilePath = new org.apache.hadoop.fs.Path(inputFilePathName)
      val fileNames = inputFilePathName.split("/")(7)
      val archivePathName = new org.apache.hadoop.fs.Path(outputFile + fileNames)

      println(fileNames)
      println(inputFilePathName)
      println(outputFile)
      println(archivePathName)

      if (hadoopFS.exists(inputFilePath)) {
        hadoopFS.rename(inputFilePath, archivePathName)
        println("Finish Renaming")
      }
      else {
        logger.warn("file :" + inputFilePath + " Already Not Exists !!!")
      }

    }
    catch {
      case e: Exception =>
        logger.error("Processing files", e)
        logger.error("##### Files not processed #####")
    }

  }

  /**
    * @author Mostafa Alaa 
    * @param inputDir        :
    * @param archivePath     :
    * @param backlogFilePath :
    * @since 1.0
    */
  def handleOldFiles(inputDir: String, archivePath: String, backlogFilePath: String) = {

    val tempFileName = "temp_" + batchIdFormat.format(Calendar.getInstance().getTime)

    var files = List[String]()
    logger.info("Starting Load Not Processed File(s): ")

    val oldFiles: List[File] = utils.listFilesInDir(inputDir.replace("hdfs://biprd", "/bigpfsprod"))
    //val oldFiles: List[File] = utils.listFilesInDir(inputDir.replace("hdfs://aubinsnp",
      //                                                                 "/rtmstaging"))

    logger.info("List of Old files  " + oldFiles.foreach(x => println(x.getPath)))

    oldFiles.foreach(
                      f => {
                        logger.info("Filter for old data only ")

                        if (!files.contains(f.toString)) {
                          logger.info("1- Read All the Record inside the file :" + f.toString)
                          val fileContent: List[String] = utils.readFiles(f.toString)

                          logger.info("2- Add File data into temp List ")
                          files = files ::: fileContent

                          logger.info("files count = " + files.size)

                        }

                      })

    logger.info("3- Create new file contains old data from temp List ")
    files.foreach(
                   lines => {
                     utils.appendFile(
                                       inputDir
                                         //.replace("hdfs://biprd", "/bigpfsprod") +tempFileName,
                                         .replace("hdfs://aubinsnp", "/rtmstaging")+tempFileName,
                                       lines, true)
                   })

    logger.info("4- Move old files to Archive ")
    oldFiles
      .foreach(x => utils.moveFiles(x.getPath, x.getPath.replace("RAW_FILES", "RAW_FILES_ARCHIVE")))

    logger.info("5- Add the temp List into processed file log ")
    utils.appendFile(backlogFilePath, tempFileName, true)

  }
}
