package com.arquivei.core.io.db.bigquery

import com.google.cloud.bigquery.LegacySQLTypeName
import org.scalatest.FlatSpec

class ToSchemaTest extends FlatSpec {


  "ToSchema" should "convert tuple list to Schema" in {
    val table = List(
      ("field1", "STRING"),
      ("field2", "INTEGER"),
      ("field3", "BOOLEAN")
    )

    val schema = ToSchema(table)

    val fields = schema.getFields
    assertResult(3)(fields.size())

    assertResult("field1")(fields.get(0).getName)
    assertResult(LegacySQLTypeName.STRING)(fields.get(0).getType)

    assertResult("field2")(fields.get(1).getName)
    assertResult(LegacySQLTypeName.INTEGER)(fields.get(1).getType)

    assertResult("field3")(fields.get(2).getName)
    assertResult(LegacySQLTypeName.BOOLEAN)(fields.get(2).getType)
  }

}
