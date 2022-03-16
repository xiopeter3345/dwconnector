package com.microsoft.synapse

import com.microsoft.synapse.utils.{DWConnectorShutdownHookManager, SynapseJDBCUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.slf4j.{Logger, LoggerFactory}

class SQLDWRelation(paramSQLContext: SQLContext, parameters: Map[String, String]) extends BaseRelation with TableScan {

  private lazy val logger: Logger = LoggerFactory.getLogger(classOf[SQLDWRelation])

  override def buildScan(): RDD[Row] = {
    val spark = sqlContext.sparkSession



    // create external table to pull data
    val stagingDir = SynapseJDBCUtils.createExternalTableAndGetStorageLocation(parameters: Map[String, String])

    // register shutdown hook
    DWConnectorShutdownHookManager.addDWConnectorShutdownHook(spark.sparkContext.hadoopConfiguration)
    DWConnectorShutdownHookManager.registerStagingDeleteDir(stagingDir)

    // read parquet file
    spark.read.schema(schema).parquet(stagingDir).rdd

  }

  override def sqlContext: SQLContext = paramSQLContext

  override def schema: StructType = {
    // get schema

    SynapseJDBCUtils.getSchema(parameters).get
  }

  def insert(pathToParquet: String, parameters: Map[String, String], overwrite: Boolean, schema:StructType): Unit = {

    var preQueries: Array[String] = Array()
    if (overwrite) {
      val options:JDBCOptions = {
        if(!parameters.contains("table_name"))
          new JDBCOptions(parameters)
        else
          new JDBCOptions(parameters("url"), parameters("table_name"), parameters)
      }
      val schemaString = SynapseJDBCUtils.getSchemaString(schema, options)
      logger.info("overwrite mode, create a new table with schema if table does not exist")
      logger.info(s"Table schema: $schemaString")

      preQueries = preQueries ++ Array(SynapseJDBCUtils.dropTableScript(parameters), SynapseJDBCUtils.createTableScript(parameters, schemaString))

    }

    SynapseJDBCUtils.copyIntoDWTable(pathToParquet, parameters, preQueries)
  }
}
