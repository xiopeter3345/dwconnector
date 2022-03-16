package com.microsoft.synapse.utils

import org.apache.http.NameValuePair
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.HttpClients
import org.apache.http.message.BasicNameValuePair
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types._
import org.slf4j.{Logger, LoggerFactory}

import java.net.URL
import java.sql.{Connection, JDBCType, ResultSet, ResultSetMetaData, SQLException, Statement, Types}
import java.util
import java.util.UUID
import scala.io.Source

object SynapseJDBCUtils {

  private lazy val logger: Logger = LoggerFactory.getLogger(SynapseJDBCUtils.getClass)

  private lazy val dropTableScript: String = "if object_id('%s') is not null\n" +
    "begin\n" +
    "drop table %s\n" +
    "end\n"

  private lazy val createTableScript: String = "if object_id('%s') is null\n" +
  "begin\n" +
  "create table %s (%s) with (%s)\n" +
  "end\n"

  private lazy val getExternalSourcePathScript: String =
    "select location from sys.external_data_sources where name = '%s'"

  def createConnection(options: Map[String, String]): Connection = {
    try {

      val classLoader = Option(Thread.currentThread().getContextClassLoader)
        .getOrElse(this.getClass.getClassLoader)
      val driverClass = Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver", true, classLoader).getName
      var jdbcOptionsInput = Map[String, String]()
      val url: String = options("url")

      jdbcOptionsInput += ("driver" -> driverClass)
      jdbcOptionsInput += ("url" -> url)
      jdbcOptionsInput += (
        if(!options.contains("dbtable"))
          "query" -> options("query")
        else
          "dbtable" -> options("dbtable")
        )
      if(options.contains("clientid"))
        if(options.contains("clientsecret")) {
          if (options.contains("authendpoint")) {
            val clientId: String = options("clientid")
            val clientSecret: String = options("clientsecret")
            val authEndpoint: String = options("authendpoint")
            val httpPost = new HttpPost(authEndpoint)
            val params = new util.ArrayList[NameValuePair]()
            params.add(new BasicNameValuePair("client_id", clientId))
            params.add(new BasicNameValuePair("client_secret", clientSecret))
            params.add(new BasicNameValuePair("grant_type", "client_credentials"))
            params.add(new BasicNameValuePair("resource", "https://sql.azuresynapse.net"))
            val httpClient = HttpClients.createDefault()
            httpPost.setEntity(new UrlEncodedFormEntity(params))
            val response = httpClient.execute(httpPost)
            if (response.getStatusLine.getStatusCode < 300) {
              val responseStr = Source.fromInputStream(response.getEntity.getContent).mkString
              val props = responseStr.substring(1, responseStr.length - 1).split(",")
              for (i <- props) {
                val prop = i.split(":")
                if (prop(0).equals("\"access_token\""))
                  jdbcOptionsInput += "accessToken" -> prop(1).substring(1, prop(1).length - 1)
              }
            } else
              throw DWConnectorException(response.getStatusLine.getReasonPhrase)
          } else
            throw DWConnectorException("clientId, clientSecret, authEndpoint must be specified together")
        } else
          throw DWConnectorException("clientId, clientSecret, authEndpoint must be specified together")
      else if(options.contains("user")) {
          if (options.contains("password")) {
            jdbcOptionsInput += "user" -> options("user")
            jdbcOptionsInput += "password" -> options("password")
          } else
            throw DWConnectorException("user, password must be specified together")
        }
        else if(options.contains("accesstoken")) {
          jdbcOptionsInput += "accessToken" -> options("accesstoken")
        } else
          throw DWConnectorException("Authentication to DW options must be specified")


      val jdbcOptions: JDBCOptions = new JDBCOptions(jdbcOptionsInput)
      val connection = JdbcUtils.createConnectionFactory(jdbcOptions)()
      connection

    } catch {
      case e: ClassNotFoundException =>
        throw e
      case e: SQLException =>
        throw e
    }
  }

  def copyIntoDWTable(location: String, parameters: Map[String, String], preQueries: Array[String]):Unit = {

    logger.info(s"Queries prior to copy into ${ for(query <- preQueries) println(query)}")
    val tableName:String = parameters("dbtable")

    val conn = createConnection(parameters)

    val url = new URL(location)
    val fullPath = url.getPath
    val containerAndAccount = url.getAuthority
    val containerAndPath = containerAndAccount.split("@")
    val containerName = containerAndPath(0)
    val accountName = containerAndPath(1)
    val pathToParquet = "https://" + accountName + "/" + containerName + fullPath

    val copyIntoScript =
      s"COPY INTO $tableName FROM '$pathToParquet' WITH (FILE_TYPE = 'parquet', CREDENTIAL=(IDENTITY='Managed Identity'))"

    logger.info(s"Run copy script: $copyIntoScript")
    var allQueries = preQueries ++ Array(copyIntoScript)

    executeSqlBatch(conn, allQueries)
  }

  def getSchemaString(schema: StructType, options:JDBCOptions): String = {
    val schemaString:StringBuilder = new StringBuilder("")
    val dialect = JdbcDialects.get(options.url)
    val fields:Array[StructField] = schema.fields
    for(i <- 0 until fields.length - 1) {
      val name = fields(i).name
      val dataType = fields(i).dataType
      val nullable = fields(i).nullable
      val typeString = dialect.getJDBCType(dataType).getOrElse(getJDBCType(dataType)).databaseTypeDefinition
      val fieldString = name + " " + typeString + " " + (if (nullable) "null" else "not null")
      schemaString ++= fieldString
      if(i != fields.length - 1)
        schemaString ++= ","
    }
    schemaString.toString()
  }

  def dropTableScript(parameters: Map[String, String]): String = {
      val dropScript = dropTableScript.format(parameters("dbtable"), parameters("dbtable"))
      logger.info("drop table script: %s".format(dropScript))
      dropScript
  }

  def createTableScript(parameters: Map[String, String], schemaString: String): String = {
    if(parameters.contains("tableoptions")) {
      val tableOptions = parameters("tableoptions")
      if(tableOptions.toUpperCase.indexOf("HASH(") != -1) {
        var distributionColumn = tableOptions.substring(
          tableOptions.toUpperCase.indexOf("HASH(") + "HASH(".length,
        tableOptions.indexOf(")", tableOptions.toUpperCase.indexOf("HASH(")))
        if(distributionColumn.indexOf("[") != -1)
          distributionColumn = distributionColumn.substring(1, distributionColumn.length - 1)
        logger.info(s"Table options with distributed ( $distributionColumn ) and schemaString ($schemaString)")
        if(!schemaString.toLowerCase.contains(distributionColumn.toLowerCase))
          throw DWConnectorException(s"Distributed column $distributionColumn is not in schema ($schemaString)")
      }
    }

    val createScript = createTableScript.format(parameters("dbtable"), parameters("dbtable"),
      schemaString, parameters.getOrElse("tableoptions", "HEAP, DISTRIBUTION = ROUND_ROBIN"))
    logger.info("create table script: %s".format(createScript))
    createScript
  }

  def getSchema(parameters: Map[String, String]): Option[StructType] = {

    val options:JDBCOptions = {
      if(!parameters.contains("dbtable"))
        new JDBCOptions(parameters)
      else
        new JDBCOptions(parameters("url"), parameters("dbtable"), parameters)
    }
    val conn = createConnection(parameters)
    val dialect = JdbcDialects.get(options.url)
    try {
      val statement = conn.prepareStatement(dialect.getSchemaQuery(options.tableOrQuery))
      try {
        statement.setQueryTimeout(options.queryTimeout)
        val rs = statement.executeQuery()
        val metadata = rs.getMetaData
        val numColumns = metadata.getColumnCount
        val fields = new Array[StructField](numColumns)
        var i = 0
        while (i < numColumns) {
          val columnName = metadata.getColumnLabel(i + 1)
          val dataType = metadata.getColumnType(i + 1)
          val typeName = metadata.getColumnTypeName(i + 1)
          val fieldSize = metadata.getPrecision(i + 1)
          val fieldScale = metadata.getScale(i + 1)
          val isSigned = {
            try {
              metadata.isSigned(i + 1)
            } catch {
              case e: SQLException
                if e.getMessage == "Method not supported" &&
                  metadata.getClass.getName == "org.apache.hive.jdbc.HiveResultSetMetaData" =>
                true
            }
          }
          val nullable = metadata.isNullable(i + 1) != ResultSetMetaData.columnNoNulls
          val resultMeta = new MetadataBuilder().putLong("scale", fieldScale)
          val columnType = dialect.getCatalystType(dataType, typeName, fieldSize, resultMeta)
            .getOrElse(getCatalystType(dataType, fieldSize, fieldScale, isSigned))
          fields(i) = StructField(columnName, columnType, nullable)
          i = i + 1
        }
        Some(new StructType(fields))
      } catch {
        case _: SQLException => None
      } finally {
        statement.close()
      }
    } catch {
      case _: SQLException => None
    }
  }

  private def getCatalystType(
                               sqlType: Int,
                               precision: Int,
                               scale: Int,
                               signed: Boolean): DataType = {
    val answer = sqlType match {
      case java.sql.Types.ARRAY => null
      case java.sql.Types.BIGINT => if (signed) {
        LongType
      } else {
        DecimalType(20, 0)
      }
      case java.sql.Types.BINARY => BinaryType
      case java.sql.Types.BIT => BooleanType // @see JdbcDialect for quirks
      case java.sql.Types.BLOB => BinaryType
      case java.sql.Types.BOOLEAN => BooleanType
      case java.sql.Types.CHAR => StringType
      case java.sql.Types.CLOB => StringType
      case java.sql.Types.DATALINK => null
      case java.sql.Types.DATE => TimestampType
      case java.sql.Types.DECIMAL if precision != 0 || scale != 0 => DecimalType(precision, scale)
      case java.sql.Types.DECIMAL => DecimalType.SYSTEM_DEFAULT
      case java.sql.Types.DISTINCT => null
      case java.sql.Types.DOUBLE => DoubleType
      case java.sql.Types.FLOAT => FloatType
      case java.sql.Types.INTEGER => if (signed) {
        IntegerType
      } else {
        LongType
      }
      case java.sql.Types.JAVA_OBJECT => null
      case java.sql.Types.LONGNVARCHAR => StringType
      case java.sql.Types.LONGVARBINARY => BinaryType
      case java.sql.Types.LONGVARCHAR => StringType
      case java.sql.Types.NCHAR => StringType
      case java.sql.Types.NCLOB => StringType
      case java.sql.Types.NULL => null
      case java.sql.Types.NUMERIC if precision != 0 || scale != 0 => DecimalType(precision, scale)
      case java.sql.Types.NUMERIC => DecimalType.SYSTEM_DEFAULT
      case java.sql.Types.NVARCHAR => StringType
      case java.sql.Types.OTHER => null
      case java.sql.Types.REAL => DoubleType
      case java.sql.Types.REF => StringType
      case java.sql.Types.REF_CURSOR => null
      case java.sql.Types.ROWID => LongType
      case java.sql.Types.SMALLINT => IntegerType
      case java.sql.Types.SQLXML => StringType
      case java.sql.Types.STRUCT => StringType
      case java.sql.Types.TIME => TimestampType
      case java.sql.Types.TIME_WITH_TIMEZONE => null
      case java.sql.Types.TIMESTAMP => TimestampType
      case java.sql.Types.TIMESTAMP_WITH_TIMEZONE => null
      case java.sql.Types.TINYINT => IntegerType
      case java.sql.Types.VARBINARY => BinaryType
      case java.sql.Types.VARCHAR => StringType
      case _ =>
        throw new SQLException("Unrecognized SQL type " + sqlType)
    }

    if (answer == null) {
      throw new SQLException("Unsupported type " + JDBCType.valueOf(sqlType).getName)
    }
    answer
  }

  private def getJDBCType(dataType: DataType): org.apache.spark.sql.jdbc.JdbcType = {
    val answer = dataType match {
      case LongType => org.apache.spark.sql.jdbc.JdbcType("bigint", Types.BIGINT)
      case IntegerType => org.apache.spark.sql.jdbc.JdbcType("int", Types.INTEGER)
      case BinaryType => org.apache.spark.sql.jdbc.JdbcType("varbinary", Types.VARBINARY)
      case BooleanType => org.apache.spark.sql.jdbc.JdbcType("bit", Types.BIT)
      case StringType => org.apache.spark.sql.jdbc.JdbcType("nvarchar", Types.NVARCHAR)
      case TimestampType => org.apache.spark.sql.jdbc.JdbcType("datetime", Types.TIMESTAMP)
      case DecimalType.SYSTEM_DEFAULT => org.apache.spark.sql.jdbc.JdbcType("decimal", Types.DECIMAL)
      case DoubleType => org.apache.spark.sql.jdbc.JdbcType("float", Types.FLOAT)
      case FloatType => org.apache.spark.sql.jdbc.JdbcType("float", Types.FLOAT)
      case _ =>
        throw new Exception("Unrecognized SQL type " + dataType)
    }

    if (answer == null) {
      throw new Exception("Unsupported type " + dataType.typeName)
    }
    answer
  }

  def createExternalTableAndGetStorageLocation(parameters: Map[String, String]): String = {
    val uniqueId: String = UUID.randomUUID().toString.replace("-", "_")

    val tableName = parameters.getOrElse("dbtable", parameters("query"))
    val stagingPath = parameters.getOrElse("stagingdir", "")
    val conn = createConnection(parameters)

    // create database scoped credential
    val dbScopedCredentialScript = s"CREATE DATABASE SCOPED CREDENTIAL DWConnector_credential_$uniqueId WITH IDENTITY = 'Managed Service Identity'"

    // path to parquet
    val pathToParquet = stagingPath + "/" + "DWConnector_" + uniqueId

    // create external data source
    val externalDataSourceScript = s"CREATE EXTERNAL DATA SOURCE DWConnector_external_data_source_$uniqueId WITH (LOCATION = '$pathToParquet', CREDENTIAL=DWConnector_credential_$uniqueId, TYPE=HADOOP)"

    // create external file format
    val externalFileFormatScript = s"CREATE EXTERNAL FILE FORMAT DWConnector_external_file_format_$uniqueId WITH (FORMAT_TYPE = PARQUET)"

    // create external table
    val externalTableScript = {
      val tableScript = s"CREATE EXTERNAL TABLE DWConnector_external_table_$uniqueId "
      val dataSourceScript = {
        if(!parameters.contains("externaldatasource")) {
          s"WITH (LOCATION = '/', DATA_SOURCE = DWConnector_external_data_source_$uniqueId, "
        } else {
          s"WITH (LOCATION = '/$uniqueId', DATA_SOURCE = ${parameters("externaldatasource")}, "
      }}
      val fileFormatScript = {
        if(!parameters.contains("externalfileformat")) {
          s"FILE_FORMAT = DWConnector_external_file_format_$uniqueId) as select * from ($tableName) as r"
        } else {
          s"FILE_FORMAT = ${parameters("externalfileformat")}) as select * from ($tableName) as r"
        }
      }
      tableScript + dataSourceScript + fileFormatScript
    }

    // drop external table
    val dropExternalTableScript = s"DROP EXTERNAL TABLE DWConnector_external_table_$uniqueId"

    // drop external file format
    val dropExternalFileFormatScript = s"DROP EXTERNAL FILE FORMAT DWConnector_external_file_format_$uniqueId"

    // drop external data source
    val dropExternalDataSourceScript = s"DROP EXTERNAL DATA SOURCE DWConnector_external_data_source_$uniqueId"

    // drop database scoped credential
    val dropDatabaseScopedCredentialScript = s"DROP DATABASE SCOPED CREDENTIAL DWConnector_credential_$uniqueId"

    var queries:Array[String] = Array[String]()
    if(!parameters.contains("externaldatasource")) {
      queries = queries ++ Array(dbScopedCredentialScript, externalDataSourceScript)
    }
    if(!parameters.contains("externalfileformat")) {
      queries = queries ++ Array(externalFileFormatScript)
    }
    queries = queries ++ Array(externalTableScript, dropExternalTableScript)
    if(!parameters.contains("externalfileformat")) {
      queries = queries ++ Array(dropExternalFileFormatScript)
    }
    if(!parameters.contains("externaldatasource")) {
      queries = queries ++ Array(dropExternalDataSourceScript, dropDatabaseScopedCredentialScript)
    }
    //val queries:Array[String] = Array[String](dbScopedCredentialScript, externalDataSourceScript, externalFileFormatScript, externalTableScript,
    //  dropExternalTableScript, dropExternalFileFormatScript, dropExternalDataSourceScript, dropDatabaseScopedCredentialScript)

    logger.info(s"Script to send data to storage through polybase: ${ for(query <- queries) logger.info(query)}")
    executeSqlBatch(conn, queries)

    if(!parameters.contains("externaldatasource")) {
      logger.info(s"Output staging path: $pathToParquet")
      pathToParquet

    } else {
      val getLocationOfExternalDataSource = getExternalSourcePathScript.format(parameters("externaldatasource"))
      val rs = executeSQLCommandWithResultSet(conn, getLocationOfExternalDataSource)
      val location:String = {
        rs.next
        rs.getString(1)
      }
      logger.info(s"Output staging path: $location/$uniqueId")
      s"$location/$uniqueId"

    }
  }

  def executeSQLCommandWithResultSet(conn: Connection, query:String): ResultSet = try {
    val stat:Statement = conn.createStatement()
    stat.executeQuery(query)
  } catch {
    case e: SQLException => throw e
  }

  def executeSQLCommand(conn: Connection, query: String): Boolean = {
    try {
      val stat: Statement = conn.createStatement()
      stat.execute(query)
    } catch {
      case e: SQLException => throw e
    }
  }

  def executeSqlBatch(conn: Connection, queries: Array[String]): Unit = try {
      val stat: Statement = conn.createStatement()
      for(query <- queries)
        stat.addBatch(query)
      stat.executeBatch()
      stat.close()
    } catch {
      case e: SQLException => throw e
    }

  def getLocationOfExternalDataSource(parameters: Map[String, String]): String = {
    val conn = createConnection(parameters)
    val getLocationOfExternalDataSource = getExternalSourcePathScript.format(parameters("externaldatasource"))
    val rs = executeSQLCommandWithResultSet(conn, getLocationOfExternalDataSource)
    val location:String = {
      rs.next
      rs.getString(1)
    }
    location
  }

}
