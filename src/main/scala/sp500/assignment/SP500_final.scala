/**
  * Created by oleg.baydakov on 22/07/2017.
  */

package sp500.assignment

import java.sql.Date

import org.apache.commons.math3.distribution.TDistribution
import org.apache.commons.math3.exception.MathIllegalArgumentException
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


final case class Record(Date: Date, SP500: Option[Double])

object SP500_final extends InitSpark {
  import spark.implicits._

  def main(args: Array[String]):Unit = {
    val filePath="src/main/resources/SP500.csv"
    val confidenceLevel=0.90
    //val filePath=args{0}
    //val confidenceLevel=args{1}.toDouble

    val df_final: DataFrame = readAndTransform(filePath)
    val ci = calcMeanCI(df_final, confidenceLevel)

    println(f"For confidence level $confidenceLevel%s confidence interval is [${ci._1}%.4f , ${ci._2}%.4f]")
    close
  }
  def readAndTransform(filePath:String):DataFrame =
  {
    val schema: StructType = StructType(Array(StructField("Date", DateType, false),
                                        StructField("SP500", DoubleType, false)))
    // read data from csv file
    val df: Dataset[Record] = reader.schema(schema).csv(filePath).na.drop().as[Record]

    // create new columns to store absolute and percent differences between  SP500 current  and previous rows
    val window: WindowSpec = Window.orderBy("Date")
    val lagCol: Column = lag(col("SP500"), 1).over(window)
    val differencePercent: Column = (col("SP500") - col("PrevSP500"))/ col("SP500")*100

    // calculate values in the new columns and keep only percent differences
    val dfDifference: DataFrame =df.withColumn("PrevSP500", lagCol)
      .na.drop()
      .withColumn("DiffPercent", round(differencePercent.cast(FloatType),2))
      .select(col("DiffPercent"))
    dfDifference
  }

  def calcMeanCI(stats:DataFrame, level: Double): Tuple2[Double,Double] =
    try {
      // Create T Distribution with N-1 degrees of freedom
      val tDist: TDistribution = new TDistribution(stats.count() - 1)
      // Calculate critical value
      val critVal: Double = tDist.inverseCumulativeProbability(1.0 - (1 - level) / 2)
      // Calculate confidence interval
      val stddev: Double =stats.agg(stddev_pop($"DiffPercent")).head().getDouble(0)
      val mean: Double =stats.agg(avg($"DiffPercent")).head().getDouble(0)
      val sqrtSampleSize: Double = Math.sqrt(stats.count())

      val ciLower: Double =mean-critVal * stddev / sqrtSampleSize
      val ciUpper: Double =mean+critVal * stddev / sqrtSampleSize
      (ciLower, ciUpper)

    } catch {
      case e: MathIllegalArgumentException => (java.lang.Double.NaN,java.lang.Double.NaN)

    }

}
