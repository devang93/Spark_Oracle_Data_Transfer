import OracleSparkTransfer.AppConfig
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

case class AppConfig(
                      s3path: String = "",
                      s3accessKey: String = "",
                      s3secretKey: String = "",
                      adls: String = "",
                      tenantId: String = "",
                      spnClientId: String = "",
                      spnClientSecret: String = "",
                      outputPath: String = "",
                      inputFormat: String = "",
                      inputOptions: String = "",
                      outputFormat: String = "",
                      outputOptions: String = "",
                      writeMode: String = "overwrite",
                      job_run_id: Option[Long] = None,
                      exec_date: Option[String] = None,
                      prev_exec_date: Option[String] = None
                    )

object S3toAdls {

    private val log = LoggerFactory.getLogger("S3toAdls")

    def main(args: Array[String]): Unit = {

        val parser = new scopt.OptionParser[AppConfig]("Spark S3 to ADLS Data Transfer Utility: 1.0.0") {
            head("Spark S3 to ADLS Data Transfer", "1.0.0")
            opt[String]('x', "s3path").required.valueName("<s3path>").action((x,c) => c.copy(s3path = x)).text("s3 File Path: REQUIRED")
            opt[String]('y', "s3accessKey").required.valueName("<s3accessKey").action((x,c) => c.copy(s3accessKey = x)).text("s3 Access Key: REQUIRED")
            opt[String]('z', "s3secretKey").required.valueName("<s3secretKey").action((x,c) => c.copy(s3secretKey = x)).text("s3 Secret Key: REQUIRED")
            opt[String]('o', "outputpath").required.valueName("<outputpath>").action((x,c) => c.copy(outputPath = x)).text("Output Path: REQUIRED")
            opt[String]('a', "adls").required.valueName("<adlsName>").action((x,c) => c.copy(adls = x)).text("Azure Data Lake Storage Name: REQUIRED")
            opt[String]('t', "tenantId").required.valueName("<tenantId>").action((x,c) => c.copy(tenantId = x)).text("Azure Tenant ID: REQUIRED.")
            opt[String]('k', "spnClientId").required.valueName("<spnClientId>").action((x,c) => c.copy(spnClientId = x)).text("Azure Application Service Principal Client ID: REQUIRED")
            opt[String]('s', "spnClientSecret").required.valueName("<spnClientSecret>").action((x,c) => c.copy(spnClientSecret = x)).text("Azure Application Service Principal Client Secret: REQUIRED")
            opt[String]('w', "writeMode").valueName("<writeMode>").action((x,c) => c.copy(writeMode = x)).text("Write Mode [default: overwrite, append]: OPTIONAL.")
            opt[Long]('i', "job_run_id").valueName("<job_run_id>").action((x,c) => c.copy(job_run_id = Some(x))).text("Job execution id: OPTIONAL.")
            opt[String]('e', "execution_date").valueName("<execution_date>").action((x,c) => c.copy(exec_date = Some(x))).text("Current Job execution date: OPTIONAL.")
            opt[String]('l', "prev_execution_date").valueName("<prev_execution_date>").action((x,c) => c.copy(prev_exec_date = Some(x))).text("Previous Job execution date: OPTIONAL.")
        }

        parser.parse(args, AppConfig()) match {
            case Some(config) => {

                val spark = SparkSession.builder()
                    .config("spark.hadoop.fs.s3a.access.key", config.s3accessKey)
                    .config("spark.hadoop.fs.s3a.secret.key", config.s3secretKey)
                    .config("spark.hadoop.dfs.adls.oauth2.client.id", config.spnClientId)
                    .config("spark.hadoop.dfs.adls.oauth2.credential", config.spnClientSecret)
                    .config("spark.hadoop.dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
                    .config("spark.hadoop.dfs.adls.oauth2.refresh.url", s"https://login.microsoftonline.com/${config.tenantId}/oauth2/token")
                    .appName("s3_to_adls_transfer")
                    .getOrCreate()

                val inputDF = spark.read.format("csv")
                  .option("header", "false")
                  .option("inferSchema", "false")
                  .option("delimiter", "|")
                  .load(config.s3path)
                
                inputDF.write.format("csv").option("delimiter", "|").option("compression", "gzip")
                  .save(config.adls+config.outputPath)
            }
            case None => parser.showUsageAsError
        }

    }
}

