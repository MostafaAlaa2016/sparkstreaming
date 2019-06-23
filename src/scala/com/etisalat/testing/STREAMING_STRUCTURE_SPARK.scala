package com.etisalat.testing

//import com.redislabs.provider.redis._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Encoders, SparkSession}

case class SUBREQUEST(Request_Datetime: String,
                      Transaction_Type: String,
                      Sub_Request_Type_ID: Integer,
                      Request_Date: String,
                      Account_Number: String,
                      Status: Integer)

object STREAMING_STRUCTURE_SPARK {
  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder
      .appName("SIM_SWAP")
      .config("spark.master", "local")
      // initial redis host - can be any node in cluster mode
      .config("redis.host", "px2661")
      // initial redis port
      .config("redis.port", "6379")
      .getOrCreate()

    val input_path = "file:///C://Users//mohamedamostafa//*csv"

    val output_path =
      """file:///C:/Users/mohamedamostafa/data1"""


    val schema = StructType(
      StructField("Request_Datetime", StringType, false) ::
        StructField("Transaction_Type", StringType, true) ::
        StructField("Sub_Request_Type_ID", IntegerType, false) ::
        StructField("Request_Date", StringType, true) ::
        StructField("Account_Number", StringType, false) ::
        StructField("Status", IntegerType, true) :: Nil)

    val cbcm_soh_schema = Encoders.product[SUBREQUEST].schema

    val inputDf = spark.readStream.schema(cbcm_soh_schema).option("delimiter", "|").csv(input_path)
/*
    val hashRDD = spark.sparkContext.fromRedisHash("integration:sssss").toDF("key","value")
    hashRDD.show()
    hashRDD.printSchema()
*/
/*    def getRedisDF(ss : String): RDD[(String,String)] =
    {
      //spark.sparkContext.fromRedisHash("integration:"+ ss)//.toDF("key","value")
    }*/

  /*  val RD = inputDf.select("ACCOUNT_NUMBER").foreach(x => getRedisDF(x.toString())).toString
    println(RD)*/



    val query: StreamingQuery = inputDf//.join(hashRDD)
      .writeStream.format("console").start()


    query.awaitTermination()
  }
}
