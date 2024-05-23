package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object CustLedger {
  def main(args: Array[String]): Unit = {
    //FIRST PART
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("CustLedger")
      .config("spark.hadoop.hadoop.dll.path", "")
      .master("local[*]")
      .getOrCreate()

    //Header fields missing in visits.csv, so creating schema for that
    val visitsSchema = new StructType()
      .add("visitid", IntegerType, nullable = true)
      .add("provider_id", IntegerType, nullable = true)
      .add("dateofservice", StringType, nullable = true)

    // Read the visits file into a DataFrame
    val visitsDF: DataFrame = spark.read.option("header", "false").schema(visitsSchema)
      .csv("data/visits.csv")


    // Read the providers file into a DataFrame
    val providersDF: DataFrame = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", "|")
      .csv("data/providers.csv")
    //Fields from Provider Id File: provider_id|provider_specialty|first_name|middle_name|last_name


    // Aggregate visit counts by providerid
    val visitCountsDF: DataFrame = visitsDF
      .groupBy("provider_id")
      .count()
      .withColumnRenamed("count", "Visit_Count")


    // Join visitCountsDF with providersDF
    val resultDF: DataFrame = visitCountsDF.join(providersDF, visitCountsDF("provider_id") === providersDF("provider_id"))
      .select(
        providersDF("provider_id"),
        concat(providersDF("first_name"), lit(" "), providersDF("middle_name"), lit(" "), providersDF("last_name")).alias("provider_name"),
        providersDF("provider_specialty"),
        visitCountsDF("Visit_Count")
      )
      resultDF.orderBy("provider_specialty").toJSON.show(truncate = false)



    //  Honestly the following line didn't work on my windows machine for some HDFS/Java/Windows write error,
    //  couldn't investigate for time crunch. This won't come in Unix or S3 or Blob Storage...
    //  Error=>
    //  java.lang.UnsatisfiedLinkError: org.apache.hadoop.io.nativeio.NativeIO$Windows.access0(Ljava/lang/String;I)Z
    //  resultDF.write.mode("overwrite").partitionBy("provider_specialty").json()"output/json_partitioned_by_specialty")
    //  So just showing calcs....

    // Show the result partitioned by provider_specialty
    resultDF.groupBy("provider_specialty").count()
      .select(col("provider_specialty").alias("SPECIALITY"),col("count").alias("Visit_Count"))
      .toJSON.show(truncate = false)


    //SECOND PART
    // Define a UDF to convert month number to month name
    val monthToName = udf((month: Int) => {
      month match {
        case 1 => "JAN"
        case 2 => "FEB"
        case 3 => "MAR"
        case 4 => "APR"
        case 5 => "MAY"
        case 6 => "JUN"
        case 7 => "JUL"
        case 8 => "AUG"
        case 9 => "SEP"
        case 10 => "OCT"
        case 11 => "NOV"
        case 12 => "DEC"
        case _ => "UNKNOWN"
      }
    })

    var newDF=visitsDF.withColumn("monthx", month(col("dateofservice")))
      .groupBy("provider_id","monthx").count()

    var interimDF=newDF.orderBy("monthx").withColumn("Month",monthToName(newDF("monthx")))
      .select(col("provider_id").alias("provider_id"),col("Month"),col("count").alias("Visit_Count"))


    var groupedByMonth = interimDF.groupBy("Month")
      .agg(collect_list(struct("provider_id", "Visit_Count")).alias("provider_id"))
      .orderBy("Month")

    groupedByMonth.toJSON.show()

    spark.stop()
  }
}
