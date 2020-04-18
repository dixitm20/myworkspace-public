package assignment

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object SignalProcessor extends App {

  val spark = SparkSession.builder()
    .appName("Signal Processor")
    .config("spark.master", "local[8]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
  spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

  val inputFileName = "part-00000-8b43448f-2282-4fe6-a5cc-64cdf61b750e-c000.snappy.parquet"
  val sourcePath = s"src/main/resources/data/indir/signals/$inputFileName"
  val targetPath = s"src/main/resources/data/outdir/signals"

  val srcdf = spark.read.parquet(sourcePath)

  // Choose min month item_id, In case of conflict choose lowest item_id
  val windowSpecMinMonthItem = Window
    .partitionBy("entity_id")
    .orderBy(col("month_id").asc, col("item_id").asc)

  // Choose max month item_id, In case of conflict choose lowest item_id
  val windowSpecMaxMonthItem = Window
    .partitionBy("entity_id")
    .orderBy(col("month_id").desc, col("item_id").asc)

  val minMonthItemId = row_number().over(windowSpecMinMonthItem)
  val maxMonthItemId = row_number().over(windowSpecMaxMonthItem)

  val src_ranking_df = srcdf.select(col("entity_id"),
    col("signal_count"),
    col("item_id"),
    // col("month_id"),
    // col("source"),
    minMonthItemId.alias("oldest_item_id_rank"),
    maxMonthItemId.alias("newest_item_id_rank"))
    // >> set oldest_item_id/newest_item_id to item_id
    // if rank is 1 else set to null
    // >> We will later pull the min/max of this column
    // to set oldest_item_id/newest_item_id
    .withColumn("oldest_item_id",
      when(col("oldest_item_id_rank") === 1, col("item_id")).otherwise(null))
    .withColumn("newest_item_id",
      when(col("newest_item_id_rank") === 1, col("item_id")).otherwise(null))


  val finaldf = src_ranking_df.groupBy("entity_id")
    .agg(min("oldest_item_id").alias("oldest_item_id"),
      max("newest_item_id").alias("newest_item_id"),
      sum("signal_count").alias("total_signals"))

  finaldf.show(100)

  finaldf.coalesce(1).write.mode(SaveMode.Overwrite).parquet(targetPath)

  val outputRecordCount=finaldf.count()
  print(s"Output: $outputRecordCount Records written to target dir: $targetPath")
}

