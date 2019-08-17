package com.hemanth.com

class Util {

  /*
   * Frame select query for JDBC source
   */
  def getSelectQuery(constants: Constants ): String = {
    val columns = if (constants.SOURCE_TABLE_TO_COLUMNS_SELECT == null ) "*"
    else constants.SOURCE_TABLE_TO_COLUMNS_SELECT
     if (constants.JDBC_QUERY == null ) s"""select $columns from ${constants.SOURCE_TABLE_NAME}"""
     else constants.JDBC_QUERY
  }

}
