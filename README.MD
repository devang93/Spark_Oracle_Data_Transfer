Spark Application to Load data from Oracle Databases to Azure DataLake Storage.
It uses Apache Spark JDBC read capability with Oracle JDBC drivers.
Data is written to ADLS in parquet format with snappy codec.

Dependencies:
    "org.apache.spark" %% "spark-core" % "2.1.0" % "provided"
    "org.apache.spark" %% "spark-sql" % "2.1.0" % "provided"
    "com.github.scopt" %% "scopt" % "3.3.0"  (included)
    "ojdbc7.jar"  (included)


Run Command:

    spark-submit --master <spark-master> --class OracleSparkTransfer.Main Spark_Oracle_Data_Transfer-assembly-1.0.0.jar
    --user <oracle Username>
    --password <oracle Password>
    --dbHostPort <oracle host:port (e.g. 127.0.0.1:8000)>
    --dbName <oracle Database Name>
    --query <oracle sql query to pull data>
    --adls <Azure Data Lake Store URI (e.g. adl://mydatalakestore.azuredatalakestore.net)>
    --tenantId <Azure Tenant ID>
    --spnClientId <Azure App Service Principal ApplicationID>
    --spnClientSecret <Azure App Service Principal Secret/Password>
    --outputPath <ADLS storage folder path>
    --writeMode <append | overwrite (default)> (optional)
    --numPartitions <spark parallel reads for oracle>
    --job_run_id <if job running daily then an run_id to identify individual runs> (optional)
    --exec_date <if job running daily then current execution_date for the run> (optional)
    --prev_exec_date <if job running daily then previous_execution_date for last run> (optional)

