name := "SP500"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.1.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" % "spark-sql_2.11" % sparkVersion
)

//libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.1.0"
        