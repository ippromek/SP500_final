### Calculate confidence interval for S&P500 dataset using Apache Spark.

**Assignment description:**

The Standard & Poor's 500, often abbreviated as the S&P 500, is an American stock market index representing the performance of the overall stock market in the US.

In this Assignment, the change of S&P 500 is defined as the percentage of index change compared to the previous closing index. For example, yesterday’s S&P 500 is at 1000 when stock closed; today’s S&P 500 is at 1002 when stock closed; in this case, the change of S&P 500 today is 2%.

You are required to write a Spark application in Java or Scala to find out a range [-x%, x%], so that the probability of “the change of S&P falls in this range” is around 90% statistically. For example, if 90% of the S&P change falls in the range of [-5%, 5%], your application should output 5% as the result.

Dataset can be found here [Link](https://fred.stlouisfed.org/series/SP500/downloaddata)

**Solution**

***Step #1***

Create Apache Spark instance via trait. Read csv file with feed data and convert it into Spark Dataset with case class records. 

***Step #2***

Transform dataset by calculating percent of difference between a current raw and previous one for column "SP500". 
Final dataset consists of only one column with the percentage values.

***Step #3***

For dataset column calculate lower and upper thresholds of  the confidence interval for given confidence level: 

<img src="https://s-media-cache-ak0.pinimg.com/564x/a0/3c/65/a03c650ea3ae2f6ccccfba82ba6ef5bd.jpg" width="100">

For given confidence level Z value calculated from T Distribution with N-1 degrees of freedom:

<img src="http://www.biochemia-medica.com/system/files/18(2)_Simundic_lessons_tablica1.jpg"  width="250">

Confidence interval is calculated as Tuple2  via Scala function composition  - Step1 andThen Step2 andThen Step3

**Unit Testing**

Testing process was executed with artificailly created test file (in the project resource folder). [ScalaTest](http://www.scalatest.org/) library was used to write the test cases.

***Unit #1***

Read test data file and calucalte mean value for "SP500" column. 	Test target  is to check "readFile" method.

***Unit #2***

Read test data file, add the new column to store percent of difference and calucalte mean value for "DiffPercent" column. Objective of testing is to check "transformDataframe" method.

***Unit #3***

Calculate confidence interval for the test dataset ("DiffPercent" column) with confidence level equals 0.9. Test target is to check "calcMeanCI" method.
