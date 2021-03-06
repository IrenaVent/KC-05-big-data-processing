package batch

import java.sql.Timestamp
import java.time.OffsetDateTime

import org.apache.spark.sql.{DataFrame, SparkSession}

case class AntennaMessage(year: Int, month: Int, day: Int, hour: Int, timestamp: Timestamp, id: String, metric: String, value: Long)

trait BatchJob {

  val spark: SparkSession

  def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame

  def readDataPSQL(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame

  def enrichMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame

  def hourlyTotalBytesAntenna(dataFrame: DataFrame): DataFrame

  def hourlyTotalBytesUser(dataFrame: DataFrame): DataFrame

  def hourlyTotalBytesApp(dataFrame: DataFrame): DataFrame

  def usersWithExceededQuota(dataFrame: DataFrame): DataFrame

  def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit

  def run(args: Array[String]): Unit = {
    val Array(storagePath, filterDate , jdbcUri, jdbcUserMetaTable,jdbcByteHourlyTable, quotaLimitTable, jdbcUser, jdbcPassword) = args
    println(s"Running with: ${args.toSeq}")

    val localDF = readFromStorage(storagePath, OffsetDateTime.parse(filterDate))
    val sumTotalBytesAntennaDF = hourlyTotalBytesAntenna(localDF)
    val sumTotalBytesUserDF = hourlyTotalBytesUser(localDF)
    val sumTotalBytesAppDF = hourlyTotalBytesApp(localDF)
    val hourlyBytesDataDF = readDataPSQL(jdbcUri, jdbcByteHourlyTable, jdbcUser, jdbcPassword)
    val userMetadataDF = readDataPSQL(jdbcUri, jdbcUserMetaTable, jdbcUser, jdbcPassword)
    val enrichMetadataDF = enrichMetadata(userMetadataDF, hourlyBytesDataDF).cache()
    val exceededQuotaDF = usersWithExceededQuota(enrichMetadataDF)

    writeToJdbc(sumTotalBytesAntennaDF, jdbcUri, jdbcByteHourlyTable, jdbcUser, jdbcPassword)
    writeToJdbc(sumTotalBytesUserDF, jdbcUri, jdbcByteHourlyTable, jdbcUser, jdbcPassword)
    writeToJdbc(sumTotalBytesAppDF, jdbcUri, jdbcByteHourlyTable, jdbcUser, jdbcPassword)
    writeToJdbc(exceededQuotaDF, jdbcUri, quotaLimitTable, jdbcUser, jdbcPassword)

    spark.close()
  }

}
