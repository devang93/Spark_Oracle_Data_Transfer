name := "Spark_Oracle_Data_Transfer"

version := "1.0.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0",
  "org.apache.spark" %% "spark-sql" % "2.1.0",
  "com.github.scopt" %% "scopt" % "3.3.0"
)