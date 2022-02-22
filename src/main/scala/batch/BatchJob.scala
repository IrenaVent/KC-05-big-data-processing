package batch

import java.sql.Timestamp
import java.time.OffsetDateTime

import org.apache.spark.sql.{DataFrame, SparkSession}

case class AntennaMessage(year: Int, month: Int, day: Int, hour: Int, timestamp: Timestamp, id: String, metric: String, value: Long)

trait BatchJob {

  val spark: SparkSession

  def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame

  def readUserMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame

  def enrichAntennaWithMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame

  def hourlyTotalBytesAntenna(dataFrame: DataFrame): DataFrame

  def hourlyTotalBytesUser(dataFrame: DataFrame): DataFrame

  def hourlyTotalBytesApp(dataFrame: DataFrame): DataFrame

  def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit

  def run(args: Array[String]): Unit = {
    val Array(filterDate, storagePath, jdbcUri, jdbcMetadataTable, aggJdbcTable, aggJdbcErrorTable, aggJdbcPercentTable, jdbcUser, jdbcPassword) = args
    println(s"Running with: ${args.toSeq}")

    val antennaDF = readFromStorage(storagePath, OffsetDateTime.parse(filterDate))
    val metadataDF = readUserMetadata(jdbcUri, jdbcMetadataTable, jdbcUser, jdbcPassword)
    val antennaMetadataDF = enrichAntennaWithMetadata(antennaDF, metadataDF).cache()
    val aggByCoordinatesDF = hourlyTotalBytesAntenna(antennaMetadataDF)
    val aggErrorAntennaDF = hourlyTotalBytesUser(antennaMetadataDF)
    val aggPercentStatusDF = hourlyTotalBytesApp(antennaMetadataDF)

    writeToJdbc(aggByCoordinatesDF, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)
    writeToJdbc(aggPercentStatusDF, jdbcUri, aggJdbcPercentTable, jdbcUser, jdbcPassword)
    writeToJdbc(aggErrorAntennaDF, jdbcUri, aggJdbcErrorTable, jdbcUser, jdbcPassword)

    spark.close()
  }

}
