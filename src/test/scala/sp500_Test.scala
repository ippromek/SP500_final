/**
  * Created by oleg.baydakov on 23/07/2017.
  */
import org.scalatest.FunSuite
import sp500.assignment.{Record, SP500_final}
import org.apache.spark.sql.{DataFrame, _}
import org.apache.spark.sql.functions._

class sp500_Test extends FunSuite  {

 val filePath = "src/main/resources/SP500_test.csv"
 implicit val confidenceLevel: Double = 0.9

  def ~=(x: Double, y: Double, precision: Double): Boolean = {
    if ((x - y).abs < precision) true else false
  }

  val dsFile: Dataset[Record] = SP500_final.readFile(filePath)

  test("Mean SP500 value for the testing file should be 550") {
    val mean=dsFile.agg(avg("SP500"))
                    .head()
                    .getDouble(0)
    val expected=550
    assert(mean == expected)
  }

  test("Mean  percent of difference for the testing file should be 21.43") {
    val ds_final=SP500_final.transformDataframe(dsFile)
    val meanPercent=ds_final.agg(avg("DiffPercent"))
                            .head()
                            .getDouble(0)
    val expectedPercent=21.43
    assert(~=(meanPercent, expectedPercent, 0.01))
  }

  test("Confidence interval for the testing file should be [13.8048% , 29.0619%]") {
    val expected_ciLower=13.8048
    val expected_ciUpper=29.0619
    val ds_final=SP500_final.transformDataframe(dsFile)
    val CI = SP500_final.calcMeanCI(ds_final)
    assert(~=(CI._1, expected_ciLower, 0.0001))
    assert(~=(CI._2, expected_ciUpper, 0.0001))

  }
}