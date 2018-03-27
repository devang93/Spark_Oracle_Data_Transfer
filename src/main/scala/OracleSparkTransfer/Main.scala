package OracleSparkTransfer

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
  * Created by devang93 on 3/24/2018.
  */
object Main {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").appName("Spark_Oracle_Data_Transfer").getOrCreate()

    val jdbcURL = s"jdbc:oracle:thin:@//${args(3)}/${args(2)}"
    val oracleProperties = new Properties()
    oracleProperties.setProperty("user", args(0))
    oracleProperties.setProperty("password", args(1))
    oracleProperties.setProperty("driver", "oracle.jdbc.driver.OracleDriver")

    val oracleDF = spark.read.jdbc(url = jdbcURL, table = s"(${args(4)}) oracle_data_pull", properties = oracleProperties)

    oracleDF.show()

  }
}
