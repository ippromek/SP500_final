/**
  * Created by oleg.baydakov on 22/07/2017.
  */

package sp500.assignment

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.SparkSession

trait InitSpark {
  val spark: SparkSession = SparkSession.builder()
    .appName("Confidence interval for SP500 dataset")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext
  val sqlContext = spark.sqlContext

  def reader = spark.read
    .option("header",true)
    .option("inferSchema", false)
    .option("mode", "DROPMALFORMED")
    .option("dateFormat", "dd/MM/yyyy")

  def readerWithoutHeader = spark.read
    .option("header",true)
    .option("inferSchema", false)
    .option("mode", "DROPMALFORMED")
    .option("dateFormat", "MM/dd/yyyy")


  private def init = {
    sc.setLogLevel("ERROR")
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    LogManager.getRootLogger.setLevel(Level.ERROR)
  }

  init

  def close = {
    spark.close()
  }
}
