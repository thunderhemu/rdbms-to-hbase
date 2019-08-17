package com.hemanth.com

import java.io.InputStream
import org.apache.log4j.LogManager
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.{DataFrame, SparkSession}

class HbaseIngester(spark : SparkSession) {


  val util = new Util()
  val log = LogManager.getRootLogger
  def process(yamlStream : InputStream) : Unit = {
    val constants = new Constants(yamlStream)
    validation(constants)
    log.info("Application received valid arguments ")
    val rawDf = getDataFromJdbc(util.getSelectQuery(constants),constants)
    writeToHbase(rawDf.selectExpr( "col1", "cast(col2 as string) as col2","cast(col3 as string) as col3"),constants)
  }

  /*
   * validate the input yaml file
   */
  def validation(constants: Constants) : Unit = {
    require(constants.SOURCE_TABLE_NAME != null, "Invalid jdbc source table name, jdbc source table can't be null")
    require(constants.SOURCE_JDBC_URL != null , "Invalid JDBC URL , JDBC URL can't be null")
    require(constants.SOURCE_PASSWORD != null , "Invalid JDBC password table, password can't be null")
    require(constants.SOURCE_USER != null , "Invalid JDBC user, user name can't be null")
    require(constants.SOURCE_DRIVER !=null , "Invalid Driver name, jdbc driver can't be null")
    require(constants.HBASE_TABLE != null , "Invalid hbase table, hbase table can't be null")
  }

  /*
   * write the data to Hbase
   */
  def writeToHbase(df : DataFrame , constants: Constants ) : Unit = {
    val catalogRead =    s"""{
                            |"table":{"namespace":"default", "name":"${constants.HBASE_TABLE}"},
                            |"rowkey":"col1",
                            |"columns":{
                            |"col1":{"cf":"rowkey", "col":"col1", "type":"string"},
                            |"col2":{"cf":"d", "col":"col2", "type":"string"},
                            |"col3":{"cf":"d", "col":"col3", "type":"string"}
                            |}
                            |}""".stripMargin

    df.createTempView("temp_table")
    val writeDf = if (constants.CAST_QUERY != null ) spark.sql(constants.CAST_QUERY) else df
    log.info(" Writing data to Hbase")
    writeDf.na.fill("NA").filter("col1 is not null").
      write.options(Map(HBaseTableCatalog.tableCatalog -> catalogRead, HBaseTableCatalog.newTable -> "5")).
      format("org.apache.spark.sql.execution.datasources.hbase").save()
    spark.catalog.dropTempView("temp_table")
  }

  /*
  *  Query the jbc source and bring data
  * @query : input sql query
   */
  def getDataFromJdbc( query : String,constants: Constants)  =
    spark.sqlContext.read.format("jdbc")
      .option("url", constants.SOURCE_JDBC_URL)
      .option("driver", constants.SOURCE_DRIVER)
      .option("dbtable", "(" + query +" )")
      .option("user", constants.SOURCE_USER)
      .option("password", constants.SOURCE_PASSWORD)
      .load()

}
