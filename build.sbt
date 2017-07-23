name := "SP500"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.1.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" % "spark-sql_2.11" % sparkVersion
)
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
libraryDependencies +="org.scalactic" %% "scalactic" % "3.0.1" % "test"
//libraryDependencies += "com.holdenkarau" % "spark-testing-base_2.11" % "2.1.0_0.7.1" % "test"

