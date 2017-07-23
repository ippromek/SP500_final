/**
  * Created by oleg.baydakov on 23/07/2017.
  */

package sp500.assignment

import java.io.{File, FileNotFoundException}
import java.sql.Date

import org.apache.commons.math3.distribution.TDistribution
import org.apache.commons.math3.exception.MathIllegalArgumentException
import org.apache.spark.sql.{DataFrame, _}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

final case class Record(Date: Date, SP500: Option[Double])

object SP500_final extends InitSpark {

  import spark.implicits._

  def main(args: Array[String]): Unit = {

    // First, ensure there are 2 args
    if (args.length != 2) throw new IllegalArgumentException("Exactly 2 parameters required !")

    val filePath=args(0)
    implicit val confidenceLevel=args(1).toDouble

    //val filePath = "src/main/resources/SP500_test.csv"
    //implicit val confidenceLevel:Double = 0.9

    // Then check args values
    require(confidenceLevel >0 && confidenceLevel<1, "Confidence level should be between 0 and 1")
    if (!new File(filePath).exists) throw new FileNotFoundException("File does not exist: "+filePath)

    // Then run the main process via function composition
    val readTransformCalc = readFile _ andThen transformDataframe andThen calcMeanCI
    val ci = readTransformCalc(filePath)

    println(f"For confidence level $confidenceLevel%s confidence interval is [${ci._1}%.4f%% , ${ci._2}%.4f%%]")
    close
  }

  /**
    *
    * @param filePath - path to a file to be read
    * @return - Spark Dataset with Raw[Record]
    */
  def readFile(filePath: String): Dataset[Record] = {
    val schema: StructType = {
      StructType(Array(StructField("Date", DateType, nullable = false),
                        StructField("SP500", DoubleType, nullable = false)))
    }
    // read data from csv file
    val df: Dataset[Record] = reader
                              .schema(schema).csv(filePath)
                              .na.drop()
                              .as[Record]
                              .filter("SP500 > 0")
    df
  }

  /**
    *
    * @param df - Spark Dataset to be transformed
    * @return - Dataframe with one column holding  percent of difference between current and previous row
    */
  def transformDataframe(df: Dataset[Record]): DataFrame = {
    // create new columns to store previous SP500 value  and percent of difference
    // between  SP500 current  and previous row
    val window: WindowSpec = Window.orderBy("Date")
    val lagCol: Column = lag(col("SP500"), 1).over(window)
    val differencePercent: Column = (col("SP500") - col("PrevSP500")) / col("SP500") * 100

    // calculate values in the new columns and keep only percent of difference
    val dfDifference: DataFrame = df.withColumn("PrevSP500", lagCol)
                                    .na.drop()
                                    .withColumn("DiffPercent", round(differencePercent.cast(FloatType), 2))
                                    .select(col("DiffPercent"))
    dfDifference
  }

  /**
    *
    * @param df - Dataframe with one column with the values to be processed
    * @param confidenceLevel - confidence level (between 0 and 1) to calculate confidence interval
    * @return
    */
  def calcMeanCI(df: DataFrame)(implicit confidenceLevel:Double): (Double, Double) =
    try {
      // Create T Distribution with N-1 degrees of freedom
      val tDist: TDistribution = new TDistribution(df.count() - 1)
      // Calculate critical value
      val critVal: Double = {
        tDist.inverseCumulativeProbability(1.0 - (1 - confidenceLevel) / 2)
      }
      // Calculate standard deviation
      val stddev: Double = {
        df.agg(stddev_pop($"DiffPercent")).head().getDouble(0)
      }
      // Calculate standard mean
      val mean: Double = {
        df.agg(avg($"DiffPercent")).head().getDouble(0)
      }
      val sqrtSampleSize: Double = Math.sqrt(df.count())
      // Calculate confidence interval
      val ciLower: Double = mean - critVal * stddev / sqrtSampleSize
      val ciUpper: Double = mean + critVal * stddev / sqrtSampleSize
      (ciLower, ciUpper)

    } catch {
      case e: MathIllegalArgumentException => (java.lang.Double.NaN, java.lang.Double.NaN)

    }

}
