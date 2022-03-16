package com.microsoft.synapse

import com.microsoft.synapse.utils.{DWConnectorShutdownHookManager, SynapseJDBCUtils}
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType
import org.slf4j.{Logger, LoggerFactory}

import java.util.UUID

class DefaultSource extends RelationProvider with SchemaRelationProvider with CreatableRelationProvider with DataSourceRegister {

  private lazy val logger: Logger = LoggerFactory.getLogger(classOf[DefaultSource])

  override def shortName(): String = {
    "synapsededicatedsql"
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameters, null)
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    val stagingPath = parameters.get("stagingdir")
    val url = parameters.get("url")
    val externalDataSource = parameters.get("externaldatasource")
    (stagingPath, url, externalDataSource) match {
      case (Some(path), Some(jdbcUrl), None) => new SQLDWRelation(sqlContext, parameters)
      case (None, Some(jdbcUrl), Some(datasource)) => new SQLDWRelation(sqlContext, parameters)
      case (None, Some(jdbcUrl), None) => throw new IllegalArgumentException("The stagingDir or externalDataSource must be specified")
      case (Some(path), None, None) => throw new IllegalArgumentException("The url could not be empty")
    }
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val stagingPath = parameters.get("stagingdir")
    val url = parameters.get("url")
    val externalDataSource = parameters.get("externaldatasource")
    (url, stagingPath, externalDataSource) match {
      case (Some(url), Some(path), None) =>
        val pathToParquet = writeDataFrameToStorage(parameters("stagingdir"), sqlContext, data)
        val relation: SQLDWRelation = createRelation(sqlContext, parameters, data.schema).asInstanceOf[SQLDWRelation]
        relation.insert(pathToParquet, parameters, mode == SaveMode.Overwrite, data.schema)

        relation
      case (_, _, Some(eds)) => throw new IllegalArgumentException("externalDataSource is not supported in write")
      case (_, None, _) => throw new IllegalArgumentException("stagingDir must be specified for write")
      case _ => throw new IllegalArgumentException("url must be specified for write")
    }


  }

  def writeDataFrameToStorage(location: String, sqlContext: SQLContext, data: DataFrame):String = {
    val uniqueId: String = UUID.randomUUID().toString.replace("-", "_")
    val pathToParquet = location + "/" + "DWConnector_" + uniqueId

    DWConnectorShutdownHookManager.addDWConnectorShutdownHook(sqlContext.sparkSession.sparkContext.hadoopConfiguration)
    DWConnectorShutdownHookManager.registerStagingDeleteDir(pathToParquet)

    data.write.parquet(pathToParquet)

    pathToParquet

  }

}
