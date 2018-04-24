This is Spark Application to Read/Load data from Oracle Database to Azure DataLake Storage.

Benefits:
1. Parallel Oracle Data Loading using Spark.
2. Parquet with Snappy compression codec --> 5:1 compression ratio --> Less Storage space --> Less storage cost.
3. Support to write actual Oracle query with more than just a date column filter.
4. Data can be transferred to any ADLS.
5. Total Runtime is almost 1/10th of current java copy utility.

TODO:
Support for Azure Key Vault to read ADLS details.