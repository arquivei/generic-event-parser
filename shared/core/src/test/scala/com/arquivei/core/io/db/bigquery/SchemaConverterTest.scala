package com.arquivei.core.io.db.bigquery

import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import com.google.cloud.bigquery.Field.Mode
import com.google.cloud.bigquery.LegacySQLTypeName
import org.scalatest.FlatSpec

import scala.collection.JavaConverters._

class SchemaConverterTest extends FlatSpec {

  it should "convert from tableschema to schema" in {
    val tableSchema = new TableSchema().setFields(List(
      new TableFieldSchema().setName("field1").setType("STRING").setMode("NULLABLE"),
      new TableFieldSchema().setName("field2").setType("INTEGER").setMode("REQUIRED").setDescription("description"),
      new TableFieldSchema().setName("field3").setType("BOOLEAN").setMode("REPEATED"),
      new TableFieldSchema().setName("field4").setType("RECORD").setMode("NULLABLE").setFields(
        List(
          new TableFieldSchema().setName("field1").setType("STRING").setMode("NULLABLE"),
          new TableFieldSchema().setName("field2").setType("STRING").setMode("REQUIRED")
        ).asJava
      )
    ).asJava)

    val schema = SchemaConverter(tableSchema)

    val fields = schema.getFields
    assertResult(4)(fields.size())

    assertResult("field1")(fields.get(0).getName)
    assertResult(LegacySQLTypeName.STRING)(fields.get(0).getType)
    assertResult(Mode.NULLABLE)(fields.get(0).getMode)

    assertResult("field2")(fields.get(1).getName)
    assertResult(LegacySQLTypeName.INTEGER)(fields.get(1).getType)
    assertResult(Mode.REQUIRED)(fields.get(1).getMode)
    assertResult("description")(fields.get(1).getDescription)

    assertResult("field3")(fields.get(2).getName)
    assertResult(LegacySQLTypeName.BOOLEAN)(fields.get(2).getType)
    assertResult(Mode.REPEATED)(fields.get(2).getMode)

    assertResult("field4")(fields.get(3).getName)
    assertResult(LegacySQLTypeName.RECORD)(fields.get(3).getType)
    assertResult(Mode.NULLABLE)(fields.get(3).getMode)

    val subfields = fields.get(3).getSubFields
    assertResult(2)(subfields.size())

    assertResult("field1")(subfields.get(0).getName)
    assertResult(LegacySQLTypeName.STRING)(subfields.get(0).getType)
    assertResult(Mode.NULLABLE)(subfields.get(0).getMode)

    assertResult("field2")(subfields.get(1).getName)
    assertResult(LegacySQLTypeName.STRING)(subfields.get(1).getType)
    assertResult(Mode.REQUIRED)(subfields.get(1).getMode)
  }

}
