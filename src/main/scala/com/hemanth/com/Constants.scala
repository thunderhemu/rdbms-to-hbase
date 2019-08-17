package com.hemanth.com

import java.io.InputStream

import org.yaml.snakeyaml.Yaml

class Constants(input : InputStream) {

  private val yaml = new Yaml()
  private  var obj = new java.util.LinkedHashMap[String,String]
  obj = yaml.load(input)

  val SOURCE_TABLE_NAME = obj.getOrDefault("source_table_name",null)
  val SOURCE_DRIVER = obj.getOrDefault("source_driver",null)
  val JDBC_QUERY =  obj.getOrDefault("jdbc_query",null)
  val SOURCE_USER =    obj.getOrDefault("source_user",null)
  val SOURCE_PASSWORD =  obj.getOrDefault("source_password",null)
  val SOURCE_TABLE_TO_COLUMNS_SELECT =  obj.getOrDefault("source_columns",null)
  val SOURCE_JDBC_URL =  obj.getOrDefault("source_jdbc_url",null)
  val HBASE_TABLE = obj.getOrDefault("hbase_table_name",null)
  val CAST_QUERY = obj.getOrDefault("cast_query",null)

}