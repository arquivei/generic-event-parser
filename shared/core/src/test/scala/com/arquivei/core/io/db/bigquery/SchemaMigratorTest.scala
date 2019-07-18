package com.arquivei.core.io.db.bigquery

import java.lang.reflect.Constructor

import com.google.cloud.bigquery._
import org.scalatest.FlatSpec
import collection.JavaConverters._

class SchemaMigratorTest extends FlatSpec {
  val fields = List(
    "field1" -> "STRING",
    "field2" -> "BOOLEAN",
    "field3" -> "DATE"
  )

  private def assertSchema(schemaFields: FieldList) = {

    assertResult(3)(fields.size)

    assertResult("field1")(schemaFields.get(0).getName)
    assertResult(LegacySQLTypeName.STRING)(schemaFields.get(0).getType)

    assertResult("field2")(schemaFields.get(1).getName)
    assertResult(LegacySQLTypeName.BOOLEAN)(schemaFields.get(1).getType)

    assertResult("field3")(schemaFields.get(2).getName)
    assertResult(LegacySQLTypeName.DATE)(schemaFields.get(2).getType)
  }

  "fromFields" should "instantiate a new class correctly" in {


    val schemaMigrator = SchemaMigrator.fromFields(
      "project",
      "dataset",
      "tablename",
      fields,
      description = "description",
      isPartitioned = true
    )
    assertResult(SchemaMigrator(
      "project",
      "dataset",
      "tablename",
      schemaMigrator.schema,
      description = "description",
      isPartitioned = true
    ))(schemaMigrator)
    val schemaFields = schemaMigrator.schema.getFields

    assertSchema(schemaFields)

    val schemaMigratorClusterized = SchemaMigrator.fromFields(
      "project",
      "dataset",
      "tablename",
      fields,
      description = "description",
      isPartitioned = true,
      "field3",
      List("field1", "field2")
    )
    assertResult(SchemaMigrator(
      "project",
      "dataset",
      "tablename",
      schemaMigratorClusterized.schema,
      description = "description",
      isPartitioned = true,
      "field3",
      List("field1", "field2")
    ))(schemaMigratorClusterized)
    val schemaFieldsClusterized = schemaMigratorClusterized.schema.getFields

    assertSchema(schemaFieldsClusterized)
  }

  "migrate" should "create new table" in {
    val mock = new MigrateMockBigquery(None)
    SchemaMigrator.fromFields(
      "project",
      "dataset",
      "tablename",
      fields,
      description = "description",
    ).migrate(mock)

    assert(mock.created !== null)
    assert(mock.updated === null)
    assertResult("project")(mock.created.getTableId.getProject)
    assertResult("dataset")(mock.created.getTableId.getDataset)
    assertResult("tablename")(mock.created.getTableId.getTable)
    assertResult("description")(mock.created.getDescription)
    val timePartitioning = mock.created.getDefinition[StandardTableDefinition].getTimePartitioning
    assert(timePartitioning === null)
    val schemaFields = mock.created.getDefinition[StandardTableDefinition].getSchema.getFields
    assertSchema(schemaFields)
  }

  "migrate" should "update table" in {
    val mock = new MigrateMockBigquery(Some("project:dataset.tablename"))
    SchemaMigrator.fromFields(
      "project",
      "dataset",
      "tablename",
      fields,
      isPartitioned = true
    ).migrate(mock)

    assert(mock.created === null)
    assert(mock.updated !== null)
    assertResult("project")(mock.updated.getTableId.getProject)
    assertResult("dataset")(mock.updated.getTableId.getDataset)
    assertResult("tablename")(mock.updated.getTableId.getTable)
    val timePartitioning = mock.updated.getDefinition[StandardTableDefinition].getTimePartitioning
    assert(timePartitioning !== null)
    assertResult(TimePartitioning.Type.DAY)(timePartitioning.getType)
    val schemaFields = mock.updated.getDefinition[StandardTableDefinition].getSchema.getFields
    assertSchema(schemaFields)


    val mockClusterized = new MigrateMockBigquery(Some("project:dataset.tablename"))
    SchemaMigrator.fromFields(
      "project",
      "dataset",
      "tablename",
      fields,
      isPartitioned = true,
      partitionField = "field3",
      clusteringField = List("field1", "field2")
    ).migrate(mockClusterized)

    assert(mockClusterized.created === null)
    assert(mockClusterized.updated !== null)
    assertResult("project")(mockClusterized.updated.getTableId.getProject)
    assertResult("dataset")(mockClusterized.updated.getTableId.getDataset)
    assertResult("tablename")(mockClusterized.updated.getTableId.getTable)
    val timePartitioningClusterized = mockClusterized.updated.getDefinition[StandardTableDefinition].getTimePartitioning
    val clustering = mockClusterized.updated.getDefinition[StandardTableDefinition].getClustering
    assert(timePartitioningClusterized !== null)
    assertResult(TimePartitioning.Type.DAY)(timePartitioningClusterized.getType)
    assertResult("field3")(timePartitioningClusterized.getField)
    assertResult(List("field1", "field2").asJava)(clustering.getFields)
    val schemaFieldsClusterized = mockClusterized.updated.getDefinition[StandardTableDefinition].getSchema.getFields
    assertSchema(schemaFieldsClusterized)

  }

  class MigrateMockBigquery(existingTable: scala.Option[String]) extends MockBigQuery {
    var created: TableInfo = _
    var updated: TableInfo = _

    override def create(tableInfo: TableInfo, options: BigQuery.TableOption*): Table = {
      created = tableInfo
      null
    }

    override def update(tableInfo: TableInfo, options: BigQuery.TableOption*): Table = {
      updated = tableInfo
      null
    }

    override def getTable(tableId: TableId, options: BigQuery.TableOption*): Table = {
      if (existingTable contains s"${tableId.getProject}:${tableId.getDataset}.${tableId.getTable}") {
        val constructor = classOf[Table.Builder].getDeclaredConstructor(classOf[BigQuery], classOf[TableId], classOf[TableDefinition])
        constructor.setAccessible(true)
        val builder = constructor.newInstance(this, tableId, new TableDefinition {
          override def getType: TableDefinition.Type = ???

          override def getSchema: Schema = ???

          override def toBuilder: TableDefinition.Builder[_ <: TableDefinition, _] = ???
        })
        builder.build()
      } else {
        null
      }
    }

    override def getOptions: BigQueryOptions = null
  }

}
