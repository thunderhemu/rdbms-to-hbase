
Description :

  this bring the data from jdbc source to hbase

  Yaml Description :

  1. source_table_name ---> JDBC source table name
  2. source_driver     ---> JDBC source driver
  3. source_user       ---> JDBC source user
  4. source_password   ---> JDBC password
  5. source_columns    ---> if you want to select few columns then column name (comma separated ) else null
  6. source_jdbc_url   ---> JDBC source url
  7. hbase_table_name  ---> hbase table name
  8. jdbc_query        ---> JDBC query if needs to select few rows based on condition
  9. cast_query        ---> if you want to add columns and changes column types before ingesting hbase write query


  spark submit command :

  spark-submit --class com.hemanth.com.HbaseIngesterApp --jars <jdbc sourcer jar path>

