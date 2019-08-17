package com.hemanth.com.tests

import java.io.{File, FileInputStream, InputStream}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.{HBaseTestingUtility, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.hbase.{HBaseTableCatalog, SparkHBaseConf}
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite, Matchers}
import org.apache.hadoop.hbase.HBaseTestingUtility
import com.hemanth.com._
import scalikejdbc.{AutoSession, ConnectionPool}
import scalikejdbc._

class HbaseIngesterTest extends FunSuite with BeforeAndAfterAll with BeforeAndAfter with Matchers with Eventually {



  //val spark = SparkSession.builder.config(conf).enableHiveSupport().getOrCreate()
  private val master = "local[1]"
  private val appName = "PseudonymizerTest"
  private var spark: SparkSession = _
  private var htu = new HBaseTestingUtility
  private val columnFamilyName = "d"
  private val tableName = "testhbase"
  private val conf = new SparkConf
  conf.set(SparkHBaseConf.testConf, "true")
  private var hConf: Configuration = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    htu.startMiniCluster
    SparkHBaseConf.conf = htu.getConfiguration
    createTable(htu.getConfiguration)
    println(" - minicluster started")
    hConf = htu.getConfiguration
    conf.setMaster(master)
      .setAppName(appName)
      .set("spark.driver.allowMultipleContexts", "true")
      .set("spark.kryo.registrationRequired", "false")
      .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .set("spark.metrics.conf", "") //disable metrics for testing, otherwise it will load metrics.properties
    spark = SparkSession.builder().config(conf).getOrCreate()
    Class.forName("org.h2.Driver")

    ConnectionPool.singleton("jdbc:h2:mem:test", "user", "pass")
    implicit val session = AutoSession
    super.beforeAll()
    // create tables
    sql"""
           create table ACCOUNT_SOURCE (
                                  col1 varchar(64) not null primary key,
                                  col2 varchar(64),
                                  col3 varchar(64))""".execute.apply()

    sql"insert into ACCOUNT_SOURCE (col1,col2,col3) values ('A','Company A','test1')".update.apply()
    sql"insert into ACCOUNT_SOURCE (col1,col2,col3) values ('B','Company B','test2')".update.apply()

  }

  def createTable(hconf: Configuration): Unit = {
    val connection = ConnectionFactory.createConnection(hconf)
    val admin = connection.getAdmin
    val table = TableName.valueOf(tableName)

    if (!admin.tableExists(table)) {
      val tableDesc = new HTableDescriptor(table)
      val column = new HColumnDescriptor(columnFamilyName)
      column.setMaxVersions(5)
      tableDesc.addFamily(column)
      admin.createTable(tableDesc)
    }
  }

  override def afterAll() = {
    htu.shutdownMiniCluster()
    spark.stop
  }

  test("writeto hbase") {
    val input  : InputStream = new FileInputStream(new File("src/test/resources/input/config/test1.yaml"))
    val catalogRead =    s"""{
                            |"table":{"namespace":"default", "name":"${tableName}"},
                            |"rowkey":"col1",
                            |"columns":{
                            |"col1":{"cf":"rowkey", "col":"col1", "type":"string"},
                            |"col2":{"cf":"d", "col":"col2", "type":"string"},
                            |"col3":{"cf":"d", "col":"col3", "type":"string"}
                            |}
                            |}""".stripMargin
    new HbaseIngester(spark).process(input)
    val df = spark.read.options(Map(HBaseTableCatalog.tableCatalog -> catalogRead)).format("org.apache.spark.sql.execution.datasources.hbase").load
    df.show()
    df.printSchema()
    assert(df.count() == 2)
  }

  test("Invalid driver case") {
    val input  : InputStream = new FileInputStream(new File("src/test/resources/input/config/invalid-driver.yaml"))
    val thrown = intercept[Exception]{
      new HbaseIngester(spark).process(input)
    }
    assert(thrown.getMessage == "requirement failed: Invalid Driver name, jdbc driver can't be null")
  }

  test("Invalid hbase table case") {
    val input  : InputStream = new FileInputStream(new File("src/test/resources/input/config/invalid-hbase-table.yaml"))
    val thrown = intercept[Exception]{
      new HbaseIngester(spark).process(input)
    }
    assert(thrown.getMessage == "requirement failed: Invalid hbase table, hbase table can't be null")
  }

  test("Invalid jdbc url case") {
    val input  : InputStream = new FileInputStream(new File("src/test/resources/input/config/invalid-jdbc-url.yaml"))
    val thrown = intercept[Exception]{
      new HbaseIngester(spark).process(input)
    }
    assert(thrown.getMessage == "requirement failed: Invalid JDBC URL , JDBC URL can't be null")
  }
  test("Invalid jdbc password case") {
    val input  : InputStream = new FileInputStream(new File("src/test/resources/input/config/invalid-password.yaml"))
    val thrown = intercept[Exception]{
      new HbaseIngester(spark).process(input)
    }
    assert(thrown.getMessage == "requirement failed: Invalid JDBC password table, password can't be null")
  }

  test("Invalid jdbc source table case") {
    val input  : InputStream = new FileInputStream(new File("src/test/resources/input/config/invalid-source-table.yaml"))
    val thrown = intercept[Exception]{
      new HbaseIngester(spark).process(input)
    }
    assert(thrown.getMessage == "requirement failed: Invalid jdbc source table name, jdbc source table can't be null")
  }
  test("Invalid jdbc user case") {
    val input  : InputStream = new FileInputStream(new File("src/test/resources/input/config/invalid_user.yaml"))
    val thrown = intercept[Exception]{
      new HbaseIngester(spark).process(input)
    }
    assert(thrown.getMessage == "requirement failed: Invalid JDBC user, user name can't be null")
  }

  test("writeto hbase with out cast query") {
    val input  : InputStream = new FileInputStream(new File("src/test/resources/input/config/without-cast-query.yaml"))
    val catalogRead =    s"""{
                            |"table":{"namespace":"default", "name":"${tableName}"},
                            |"rowkey":"col1",
                            |"columns":{
                            |"col1":{"cf":"rowkey", "col":"col1", "type":"string"},
                            |"col2":{"cf":"d", "col":"col2", "type":"string"},
                            |"col3":{"cf":"d", "col":"col3", "type":"string"}
                            |}
                            |}""".stripMargin
    new HbaseIngester(spark).process(input)
    val df = spark.read.options(Map(HBaseTableCatalog.tableCatalog -> catalogRead)).format("org.apache.spark.sql.execution.datasources.hbase").load
    df.show()
    df.printSchema()
    assert(df.count() > 0)
  }

  test("writeto hbase with out jdbc query") {
    val input  : InputStream = new FileInputStream(new File("src/test/resources/input/config/without-jdbc-query.yaml"))
    val catalogRead =    s"""{
                            |"table":{"namespace":"default", "name":"${tableName}"},
                            |"rowkey":"col1",
                            |"columns":{
                            |"col1":{"cf":"rowkey", "col":"col1", "type":"string"},
                            |"col2":{"cf":"d", "col":"col2", "type":"string"},
                            |"col3":{"cf":"d", "col":"col3", "type":"string"}
                            |}
                            |}""".stripMargin
    new HbaseIngester(spark).process(input)
    val df = spark.read.options(Map(HBaseTableCatalog.tableCatalog -> catalogRead)).format("org.apache.spark.sql.execution.datasources.hbase").load
    df.show()
    df.printSchema()
    assert(df.count() > 0)
  }

  test("util without jdbc query") {
    val input  : InputStream = new FileInputStream(new File("src/test/resources/input/config/without-jdbc-query.yaml"))
    val constants = new Constants(input)
    val selectStr = new Util().getSelectQuery(constants)
    assert(selectStr == "select * from ACCOUNT_SOURCE")
  }

  test("util with jdbc query") {
    val input  : InputStream = new FileInputStream(new File("src/test/resources/input/config/test1.yaml"))
    val constants = new Constants(input)
    val selectStr = new Util().getSelectQuery(constants)
    assert(selectStr == "select * from ACCOUNT_SOURCE")
  }


}
