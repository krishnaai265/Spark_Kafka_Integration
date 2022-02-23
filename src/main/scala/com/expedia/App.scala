package com.oliver

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
/**
 * @author ${user.name}
 */
object App {
  

  def main(args : Array[String]) {

    val topicName="test-topic"

    val spark = SparkSession.builder().master("local").appName("Email_Generator")
      .getOrCreate()

    val fileSchema = StructType(Array(StructField("keychain_id", StringType),
      StructField("key_id", StringType),
      StructField("brand_name", StringType),
      StructField("site_name", StringType),
      StructField("created_date", StringType),
      StructField("updated_date", StringType),
      StructField("keychain_type", StringType),
      StructField("key_type", StringType),
      StructField("latest_visit_date", StringType)))

    val df = spark.read.schema(fileSchema).option("header", "true")
      .option("inferschema", "true")
      .csv(getClass().getClassLoader().getResource("data_2022-01-21_03_10_34_PM.csv").getPath)



    df.show()
   // df.selectExpr("to_json(struct(*)) AS value").write.format("kafka").option("topic", topicName).option("kafka.bootstrap.servers", "localhost:9092").save()
    df.selectExpr("to_json(struct(*)) AS value").writeStream
      .format("console")
      .option("truncate", false)
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start();

  }

}
