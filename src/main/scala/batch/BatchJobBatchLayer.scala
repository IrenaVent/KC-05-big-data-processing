package batch

import org.apache.spark.sql.functions.{lit, sum, window}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import streaming.StreamingJobSpeedLayer.{run, spark}

import java.time.OffsetDateTime

object BatchJobBatchLayer extends BatchJob {
  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  override def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame = {
      spark
        .read
        .format("parquet")
        .load(storagePath)
        .where(
          $"year" === lit(filterDate.getYear) &&
            $"month" === lit(filterDate.getMonthValue) &&
            $"day" === lit(filterDate.getDayOfMonth) &&
            $"hour" === lit(filterDate.getHour)
        )
  }

  override def readDataPSQL(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame = {
    spark
      .read
      .format("jdbc")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .load()
  }

  override def enrichMetadata(userMetadataDF: DataFrame, bytesHourlyDF: DataFrame): DataFrame = {
    userMetadataDF.as("a")
      .join(bytesHourlyDF.as("b"), $"a.id" === $"b.id")
      .drop($"b.id")
  }

  override def hourlyTotalBytesAntenna(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"id", $"antenna_id", $"bytes", $"app")
      .groupBy($"antenna_id", window($"timestamp", "1 hour"))
      .agg(sum("bytes").as("value"))
      .withColumn("type", lit("antenna_byte_total"))
      .select($"window.start".as("date"), $"antenna_id".as("id"), $"value", $"type")
  }

  override def hourlyTotalBytesUser(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"id", $"antenna_id", $"bytes", $"app")
      .groupBy($"id", window($"timestamp", "1 hour"))
      .agg(sum("bytes").as("value"))
      .withColumn("type", lit("user_byte_total"))
      .select($"window.start".as("date"), $"id", $"value", $"type")
  }

  override def hourlyTotalBytesApp(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"id", $"antenna_id", $"bytes", $"app")
      .groupBy($"app", window($"timestamp", "1 hour"))
      .agg(sum("bytes").as("value"))
      .withColumn("type", lit("aap_byte_total"))
      .select($"window.start".as("date"), $"app".as("id"), $"value", $"type")
  }

  override def usersWithExceededQuota(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"date", $"email", $"quota", $"value")
      .where($"value" > $"quota")
      .select($"email", $"value".as("usage"), $"quota", $"date")
  }

  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit = {
    dataFrame
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .save()
  }

  def main(args: Array[String]): Unit = run(args)

}
