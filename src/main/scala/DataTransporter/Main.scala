package OracleSparkTransfer

import java.nio.file.{Files, Paths}
import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, DecimalType, StructField, StructType}
import org.slf4j.LoggerFactory

/**
  * Created by Devang Patel on 3/24/2018.
  */

case class AppConfig(
                      master: String = "local[*]",
                      user: String = "",
                      password: String = "",
                      dbHostPort: String = "",
                      dbName: String = "",
                      query: String = "",
                      adls: String = "",
                      tenantId: String = "",
                      spnClientId: String = "",
                      spnClientSecret: String = "",
                      outputPath: String = "",
                      writeMode: String = "overwrite",
                      numPartitions: Int = 4,
                      job_run_id: Option[Long] = None,
                      exec_date: Option[String] = None,
                      prev_exec_date: Option[String] = None
                    )

object Main {

  private val log = LoggerFactory.getLogger("OracleSparkTransfer.Main")

  // function to get lower and upper bound.
  def getBounds(spark: SparkSession, jdbcUrl: String, queryString: String, oracleProperties: Properties) = {
    log.info("Getting upper and lower bound for the data to be transferred...")
    val query = s"SELECT min(ROWNUM), max(ROWNUM) FROM (${queryString})"
    val bounds = spark.read.jdbc(
                                  url = jdbcUrl,
                                  table = s"(${query}) oracle_data_count",
                                  properties = oracleProperties).take(1)
    (bounds(0)(0).toString.toDouble.toLong, bounds(0)(1).toString.toDouble.toLong)
  }

  def substituteExecutionParams(input: String, config: AppConfig): String = {
    var output = new String(input)
    config.job_run_id match {
      case Some(id) =>
        val reg = """\$job_run_id""".r
        output = reg.replaceAllIn(output, id.toString)
      case None => log.warn("No job_run_id value provided at runtime!")
    }
    config.exec_date match {
      case Some(exec_date) =>
        val reg = """\$execution_date""".r
        output = reg.replaceAllIn(output, exec_date)
      case None => log.warn("No execution_date value provided at runtime!")
    }
    config.prev_exec_date match {
      case Some(prev_exec_date) =>
        val reg = """\$prev_execution_date""".r
        output = reg.replaceAllIn(output, prev_exec_date)
      case None => log.warn("No prev_execution_date value provided at runtime!")
    }
    output
  }

  def maintest(args: Array[String]): Unit = {

    val parser = new scopt.OptionParser[AppConfig]("Spark Oracle Data Transfer Utility: 1.0.0") {
      head("Spark Oracle Data Transfer Utility", "1.0.0")
      opt[String]('u', "user").required.valueName("<user>").action((x,c) => c.copy(user = x)).text("Oracle User Name: REQUIRED")
      opt[String]('p', "password").required.valueName("<password>").action((x,c) => c.copy(password = x)).text("Oracle Password: REQUIRED")
      opt[String]('h', "hostport").required.valueName("<host:port>").action((x,c) => c.copy(dbHostPort = x)).text("Oracle Database Host:Port: REQUIRED")
      opt[String]('d', "db").required.valueName("<database>").action((x,c) => c.copy(dbName = x)).text("Oracle Database Name: REQUIRED")
      opt[String]('o', "outputpath").required.valueName("<outputpath>").action((x,c) => c.copy(outputPath = x)).text("Output Path: REQUIRED")
      opt[String]('q', "sqlquery").required.valueName("<sqlquery>").action((x,c) => c.copy(query = x)).text("Oracle SQL Query to pull data: REQUIRED")
      opt[String]('a', "adls").required.valueName("<adlsName>").action((x,c) => c.copy(adls = x)).text("Azure Data Lake Storage Name: REQUIRED")
      opt[String]('t', "tenantId").required.valueName("<tenantId>").action((x,c) => c.copy(tenantId = x)).text("Azure Tenant ID: REQUIRED.")
      opt[String]('k', "spnClientId").required.valueName("<spnClientId>").action((x,c) => c.copy(spnClientId = x)).text("Azure Application Service Principal Client ID: REQUIRED")
      opt[String]('s', "spnClientSecret").required.valueName("<spnClientSecret>").action((x,c) => c.copy(spnClientSecret = x)).text("Azure Application Service Principal Client Secret: REQUIRED")
      opt[Int]('n', "numPartitions").required.valueName("<numberPartitions>").action((x,c) => c.copy(numPartitions = x)).text("Number of Partitions for Spark parallelism: REQUIRED")
      opt[String]('w', "writeMode").valueName("<writeMode>").action((x,c) => c.copy(writeMode = x)).text("Write Mode [default: overwrite, append]: OPTIONAL.")
      opt[Long]('i', "job_run_id").valueName("<job_run_id>").action((x,c) => c.copy(job_run_id = Some(x))).text("Job execution id: OPTIONAL.")
      opt[String]('e', "execution_date").valueName("<execution_date>").action((x,c) => c.copy(exec_date = Some(x))).text("Current Job execution date: OPTIONAL.")
      opt[String]('l', "prev_execution_date").valueName("<prev_execution_date>").action((x,c) => c.copy(prev_exec_date = Some(x))).text("Previous Job execution date: OPTIONAL.")
    }

    parser.parse(args, AppConfig()) match {
      case Some(config) => {

        val spark = SparkSession.builder()
          .config("spark.hadoop.dfs.adls.oauth2.client.id", config.spnClientId)
          .config("spark.hadoop.dfs.adls.oauth2.credential", config.spnClientSecret)
          .config("spark.hadoop.dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
          .config("spark.hadoop.dfs.adls.oauth2.refresh.url", s"https://login.microsoftonline.com/${config.tenantId}/oauth2/token")
          .appName("Spark_Oracle_Data_Transfer").getOrCreate()

        val jdbcURL = s"jdbc:oracle:thin:@//${config.dbHostPort}/${config.dbName}"
        val oracleProperties = new Properties()
        oracleProperties.setProperty("user", config.user)
        oracleProperties.setProperty("password", config.password)
        oracleProperties.setProperty("driver", "oracle.jdbc.driver.OracleDriver")
        oracleProperties.setProperty("fetchsize", "2000")
        val queryString = substituteExecutionParams(new String(Files.readAllBytes(Paths.get(config.query)), "UTF-8"), config)
        log.info("Oracle Query to pull data: ")
        log.info(queryString)
        val bounds  = getBounds(spark, jdbcURL, queryString, oracleProperties)

        val oracleDF = spark.read.jdbc(url = jdbcURL,
          table = s"(SELECT ROWNUM as NUM_RECORDS, t.* FROM (${queryString}) t ) oracle_data_pull",
          columnName = "num_records",
          lowerBound = bounds._1,
          upperBound = bounds._2,
          numPartitions = config.numPartitions,
          connectionProperties = oracleProperties)

        // drop num_records column.
        val outputDF = oracleDF.drop("num_records")
        outputDF.printSchema()
        // write data out as parquet files.
        val outputPath = substituteExecutionParams(config.adls+config.outputPath, config)
        log.info("writing data from Oracle Source to Sink : "+outputPath)
        outputDF.write.mode(config.writeMode).parquet(outputPath)
      }
      case None => parser.showUsageAsError
    }

  }
}
