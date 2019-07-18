package com.arquivei.core.io.db.bigquery


import com.google.cloud.bigquery._
import org.slf4j.LoggerFactory

import collection.JavaConverters._

case class SchemaMigrator(
                           project: String,
                           dataset: String,
                           tableName: String,
                           schema: Schema,
                           description: String = null,
                           isPartitioned: Boolean = false,
                           partitionField: String = null,
                           clusteringField: List[String] = Nil
                         ) {
  def migrate(bigquery: BigQuery = BigQueryOptions.getDefaultInstance.getService): Unit = {
    val tableId = TableId.of(project, dataset, tableName)
    val tableDefinitionBuilder = StandardTableDefinition
      .newBuilder()
      .setSchema(schema)

    if (isPartitioned) {
      if (partitionField == null) {
        tableDefinitionBuilder.setTimePartitioning(TimePartitioning.of(TimePartitioning.Type.DAY))
      } else {
        tableDefinitionBuilder.setTimePartitioning(
          TimePartitioning
            .newBuilder(TimePartitioning.Type.DAY)
            .setField(partitionField)
            .build()
        )
        if (clusteringField.nonEmpty) {
          tableDefinitionBuilder.setClustering(Clustering.newBuilder().setFields(clusteringField.asJava).build())
        }
      }
    }

    val tableInfo = TableInfo
      .newBuilder(tableId, tableDefinitionBuilder.build())
      .setDescription(description)
      .build()

    val tableOpt = scala.Option(bigquery.getTable(tableId))
    tableOpt match {
      case None =>
        SchemaMigrator.LOG.info(s"Table $tableName not found, to be created!")

        bigquery.create(tableInfo)

        SchemaMigrator.LOG.info("Table created successfully!")
      case Some(_) =>
        SchemaMigrator.LOG.info(s"Table $tableName found, to be updated!")
        bigquery.update(tableInfo)

        SchemaMigrator.LOG.info("Table updated successfully!")
    }
  }

}


object SchemaMigrator {
  private val LOG = LoggerFactory.getLogger(classOf[SchemaMigrator])

  def fromFields(
                  project: String,
                  dataset: String,
                  tableName: String,
                  fields: Seq[(String, String)],
                  description: String = null,
                  isPartitioned: Boolean = false,
                  partitionField: String = null,
                  clusteringField: List[String] = Nil
                ): SchemaMigrator = SchemaMigrator(project, dataset, tableName, ToSchema(fields), description, isPartitioned, partitionField, clusteringField)
}
