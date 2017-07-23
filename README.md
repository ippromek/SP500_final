### Calculate confidence interval for S&P500 dataset using Apache Spark.

**Assignment description:**

The Standard & Poor's 500, often abbreviated as the S&P 500, is an American stock market index representing the performance of the overall stock market in the US.

In this Assignment, the change of S&P 500 is defined as the percentage of index change compared to the previous closing index. For example, yesterday’s S&P 500 is at 1000 when stock closed; today’s S&P 500 is at 1002 when stock closed; in this case, the change of S&P 500 today is 2%.

You are required to write a Spark application in Java or Scala to find out a range [-x%, x%], so that the probability of “the change of S&P falls in this range” is around 90% statistically. For example, if 90% of the S&P change falls in the range of [-5%, 5%], your application should output 5% as the result.

Dataset can be found here [Link](https://fred.stlouisfed.org/series/SP500/downloaddata)

**Step #1**
Read scv file with feed data and convert it into Spark Dataset with case class

**Step #2**
Dataset transformation includes calculating percent of difference between a current raw and previous one for column "SP500". 
Final dataset consists of only one column with the percentage values.

**Step #3**
Calculate lower and upper thresholds for the confidence interval based given confidence level. Calculation perfomrs using formula

![Formula](http://pad1.whstatic.com/images/thumb/3/37/Calculate-Confidence-Interval-Step-3-Version-4.jpg/aid1383548-v4-728px-Calculate-Confidence-Interval-Step-3-Version-4.jpg)
