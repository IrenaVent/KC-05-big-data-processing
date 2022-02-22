package batch

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.OffsetDateTime

object BatchJobBatchLayer extends BatchJob {
  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  override def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame = {
    {
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
  }

  override def readAntennaMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame = ???

  override def enrichAntennaWithMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame = ???

  override def computeDevicesCountByCoordinates(dataFrame: DataFrame): DataFrame = ???

  override def computeErrorAntennaByModelAndVersion(dataFrame: DataFrame): DataFrame = ???

  override def computePercentStatusByID(dataFrame: DataFrame): DataFrame = ???

  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit = ???

  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Unit = ???

  def main(args: Array[String]): Unit = {

    val localDF = readFromStorage("/tmp/data", OffsetDateTime.parse("2022-02-22T11:00:00Z"))

    localDF.show()

  }

}
