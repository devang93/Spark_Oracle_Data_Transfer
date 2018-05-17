name := "Spark_Oracle_Data_Transfer"

version := "1.0.1"

scalaVersion := "2.11.8"

unmanagedJars in Compile ++= Seq(
  file("lib/ojdbc7.jar"))

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.1.0" % "provided",
  "com.github.scopt" %% "scopt" % "3.3.0"
)