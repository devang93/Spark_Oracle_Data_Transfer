import org.apache.spark.sql.{DataFrame, SparkSession}

object S3toAdls {

    def main( args: Array[String]): Unit = {

        val fileFormat = "parquet"
        val spark = SparkSession.builder()
                    .config("spark.hadoop.fs.s3a.access.key", "")
                    .config("spark.hadoop.fs.s3a.secret.key", "")
                    .config("spark.hadoop.dfs.adls.oauth2.client.id", "")
                    .config("spark.hadoop.dfs.adls.oauth2.credential", "")
                    .appName("s3_to_adls_transfer")
                    .getOrCreate()
        
        var rawDF: DataFrame = null

        fileFormat match {
            case "parquet" => rawDF = spark.read.format("parquet").load("inputPath")

            case "json" => rawDF = spark.read.format("json").load("inputPath")

            case "csv" => rawDF = spark.read.format("csv")
                                    .option("header", "true")
                                    .option("inferSchema", "true")
                                    .load("inputPath")
        }


}