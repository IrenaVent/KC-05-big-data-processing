package streaming

import java.sql.Timestamp
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

import org.apache.spark.sql.{DataFrame, SparkSession}

case class devicesMessage(timestamp: Timestamp, id: String, antenna_id: String, bytes: Long, app: String)

trait StreamingJob {

  val spark: SparkSession

  def readFromKafka(kafkaServer: String, topic: String): DataFrame

  def parserJsonData(dataFrame: DataFrame): DataFrame

  def totalBytesAntenna(dataFrame: DataFrame): DataFrame

  def totalBytesUser(dataFrame: DataFrame): DataFrame

  def totalBytesApp(dataFrame: DataFrame): DataFrame

  def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit]

  def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit]

  def run(args: Array[String]): Unit = {
    val Array(kafkaServer, topic, jdbcUri, bytesJdbcTable, jdbcUser, jdbcPassword, storagePath) = args
    println(s"Running with: ${args.toSeq}")

    val kafkaDF = readFromKafka(kafkaServer, topic)
    val deviceDF = parserJsonData(kafkaDF)
    val storageFuture = writeToStorage(deviceDF, storagePath)
    val sumBytesAntennaDF = totalBytesAntenna(deviceDF)
    val sumBytesUserDF = totalBytesUser(deviceDF)
    val sumBytesAppDF = totalBytesApp(deviceDF)
    val sumFutureAntenna = writeToJdbc(sumBytesAntennaDF, jdbcUri, bytesJdbcTable, jdbcUser, jdbcPassword)
    val sumFutureUser = writeToJdbc(sumBytesUserDF, jdbcUri, bytesJdbcTable, jdbcUser, jdbcPassword)
    val sumFutureApp = writeToJdbc(sumBytesAppDF, jdbcUri, bytesJdbcTable, jdbcUser, jdbcPassword)

    Await.result(Future.sequence(Seq(sumFutureAntenna, sumFutureUser, sumFutureApp, storageFuture)), Duration.Inf)

    spark.close()
  }

}
