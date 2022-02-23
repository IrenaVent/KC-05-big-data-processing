package streaming

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.{avg, col, dayofmonth, from_json, hour, lit, max, min, month, sum, window, year}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.sql.Timestamp
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object StreamingJobSpeedLayer extends StreamingJob {

  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  override def readFromKafka(kafkaServer: String, topic: String): DataFrame = {
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("subscribe", topic)
      .load()
  }

  override def parserJsonData(dataFrame: DataFrame): DataFrame = {

    val deviceSchema: StructType = ScalaReflection.schemaFor[devicesMessage].dataType.asInstanceOf[StructType]

    dataFrame
      .select(from_json(col("value").cast(StringType), deviceSchema).as("json"))
      .select("json.*")
      .withColumn("timestamp", $"timestamp".cast(TimestampType))

//    val struct = StructType(Seq(
//      StructField("timestamp", TimestampType, nullable = false),
//      StructField("id", StringType, nullable = false),
//      StructField("antenna_id", StringType, nullable = false),
//      StructField("bytes", LongType, nullable = false),
//      StructField("app", StringType, nullable = false),
//    ))
//
//    dataFrame
//      .select(from_json($"value".cast(StringType),struct).as("value"))
//      .select($"value.*")
  }

  override def totalBytesAntenna(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"id", $"antenna_id", $"bytes", $"app")
      .withWatermark("timestamp", "15 seconds")
      .groupBy($"antenna_id", window($"timestamp", "90 seconds"))
      .agg(sum("bytes").as("value"))
      .withColumn("type", lit("antenna_byte_total"))
      .select($"window.start".as("date"), $"antenna_id".as("id"), $"value", $"type")
  }

  override def totalBytesUser(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"id", $"antenna_id", $"bytes", $"app")
      .withWatermark("timestamp", "15 seconds")
      .groupBy($"id", window($"timestamp", "90 seconds"))
      .agg(sum("bytes").as("value"))
      .withColumn("type", lit("user_byte_total"))
      .select($"window.start".as("date"), $"id", $"value", $"type")
  }

  override def totalBytesApp(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"id", $"antenna_id", $"bytes", $"app")
      .withWatermark("timestamp", "15 seconds")
      .groupBy($"app", window($"timestamp", "90 seconds"))
      .agg(sum("bytes").as("value"))
      .withColumn("type", lit("aap_byte_total"))
      .select($"window.start".as("date"), $"app".as("id"), $"value", $"type")
  }

  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit] = Future {
    dataFrame
      .writeStream
      .foreachBatch { (data: DataFrame, batchId: Long) =>
        data
          .write
          .mode(SaveMode.Append)
          .format("jdbc")
          .option("url", jdbcURI)
          .option("dbtable", jdbcTable)
          .option("user", user)
          .option("password", password)
          .save()
      }.start()
      .awaitTermination()
  }

  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit] = Future {
    val columns = dataFrame.columns.map(col).toSeq ++
      Seq(
        year($"timestamp").as("year"),
        month($"timestamp").as("month"),
        dayofmonth($"timestamp").as("day"),
        hour($"timestamp").as("hour")
      )

    dataFrame
      .select(columns: _*)
      .writeStream
      .partitionBy("year", "month", "day", "hour")
      .format("parquet")
      .option("path", s"${storageRootPath}/data")
      .option("checkpointLocation", s"${storageRootPath}/checkpoint")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = run(args)

  /*
  def main(args: Array[String]): Unit = {

    val futureTBAntenna = writeToJdbc(
      totalBytesAntenna(
        parserJsonData(
          readFromKafka(
            "34.88.239.219:9092", "devices")
        )
      ), s"jdbc:postgresql://34.122.29.249:5432/postgres", "bytes", "postgres", "keepcoding")

    val futureTBUser = writeToJdbc(
      totalBytesUser(
        parserJsonData(
          readFromKafka(
            "34.88.239.219:9092", "devices")
        )
      ), s"jdbc:postgresql://34.122.29.249:5432/postgres", "bytes", "postgres", "keepcoding")

    val futureTBApp = writeToJdbc(
      totalBytesApp(
        parserJsonData(
          readFromKafka(
            "34.88.239.219:9092", "devices")
        )
      ),s"jdbc:postgresql://34.122.29.249:5432/postgres", "bytes", "postgres", "keepcoding")

    val futureStorage = writeToStorage(parserJsonData(readFromKafka("34.88.239.219:9092", "devices")), "/tmp")

    Await.result(Future.sequence(Seq(futureTBAntenna, futureTBUser, futureTBApp, futureStorage)), Duration.Inf)
  }
  */
}
