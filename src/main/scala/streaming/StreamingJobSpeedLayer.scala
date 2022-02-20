package streaming

import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.concurrent.Future

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
    val struct = StructType(Seq(
      StructField("timestamp", TimestampType, nullable = false),
      StructField("id", StringType, nullable = false),
      StructField("antenna_id", StringType, nullable = false),
      StructField("bytes", LongType, nullable = false),
      StructField("app", StringType, nullable = false),
    ))

    dataFrame
      .select(from_json($"value".cast(StringType),struct).as("value"))
      .select($"value.*")
  }

  override def readAntennaMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame = ???

  override def enrichAntennaWithMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame = ???

  override def toDO(dataFrame: DataFrame): DataFrame = ???

  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit] = ???

  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit] = ???

  def main(args: Array[String]): Unit = {
    parserJsonData(readFromKafka("34.88.239.219:9092", "devices"))
      .writeStream
      .format(source = "console")
      .start()
      .awaitTermination()
  }
}
