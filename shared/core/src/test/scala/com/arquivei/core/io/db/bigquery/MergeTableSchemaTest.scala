package com.arquivei.core.io.db.bigquery

import java.io.StringReader

import com.arquivei.core.exceptions.ArquiveiException
import com.arquivei.core.formats.Json
import com.google.api.client.json.JsonObjectParser
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.bigquery.model.TableSchema
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.scalatest.FlatSpec

class MergeTableSchemaTest extends FlatSpec {
  "createNewTableSchema" should "merge two table schemas" in {

    lazy val jsonObjectParser = new JsonObjectParser(new JacksonFactory)
    /** Parse a schema string. */
    def parseSchema(schemaString: String): TableSchema =
      jsonObjectParser
        .parseAndClose(new StringReader(schemaString), classOf[TableSchema])

    implicit val format = org.json4s.DefaultFormats

    val event1 =
      """
        |{"fields": [
        |      {
        |        "mode": "NULLABLE",
        |        "name": "DataVersion",
        |        "type": "NUMERIC"
        |      },
        |      {
        |        "mode": "NULLABLE",
        |        "name": "Type",
        |        "type": "STRING"
        |      }
        |    ]
        |  }
      """.stripMargin

    val event2 =
      """
        |{"fields": [
        |      {
        |        "mode": "NULLABLE",
        |        "name": "Id",
        |        "type": "STRING"
        |      },
        |      {
        |        "mode": "NULLABLE",
        |        "name": "ProcessingTime",
        |        "type": "DATETIME"
        |      },
        |      {
        |        "mode": "NULLABLE",
        |        "name": "SchemaVersion",
        |        "type": "NUMERIC"
        |      },
        |      {
        |        "mode": "NULLABLE",
        |        "name": "Source",
        |        "type": "STRING"
        |      },
        |      {
        |        "mode": "NULLABLE",
        |        "name": "CreatedAt",
        |        "type": "DATETIME"
        |      },
        |      {
        |        "fields": [
        |          {
        |            "mode": "NULLABLE",
        |            "name": "AccountId",
        |            "type": "INTEGER"
        |          },
        |          {
        |            "mode": "REPEATED",
        |            "name": "Cnpjs",
        |            "type": "STRING"
        |          }
        |        ],
        |        "mode": "NULLABLE",
        |        "name": "Data",
        |        "type": "RECORD"
        |      },
        |      {
        |        "mode": "NULLABLE",
        |        "name": "IsTracking",
        |        "type": "BOOLEAN"
        |      }
        |    ]
        |  }
      """.stripMargin

    val event3 =
      """
        |{"fields": [
        |      {
        |        "mode": "NULLABLE",
        |        "name": "Id",
        |        "type": "STRING"
        |      },
        |      {
        |        "mode": "NULLABLE",
        |        "name": "ProcessingTime",
        |        "type": "DATETIME"
        |      },
        |      {
        |        "mode": "NULLABLE",
        |        "name": "SchemaVersion",
        |        "type": "NUMERIC"
        |      },
        |      {
        |        "mode": "NULLABLE",
        |        "name": "Source",
        |        "type": "STRING"
        |      },
        |      {
        |        "mode": "NULLABLE",
        |        "name": "CreatedAt",
        |        "type": "DATETIME"
        |      },
        |      {
        |        "fields": [
        |          {
        |            "mode": "NULLABLE",
        |            "name": "AccountId",
        |            "type": "INTEGER"
        |          },
        |          {
        |            "mode": "REPEATED",
        |            "name": "Cnpjs",
        |            "type": "STRING"
        |          },
        |          {
        |            "mode": "REPEATED",
        |            "name": "ExtraField",
        |            "type": "STRING"
        |          }
        |        ],
        |        "mode": "NULLABLE",
        |        "name": "Data",
        |        "type": "RECORD"
        |      },
        |      {
        |        "mode": "NULLABLE",
        |        "name": "IsTracking",
        |        "type": "BOOLEAN"
        |      }
        |    ]
        |  }
      """.stripMargin

    val expectedFinalSchemaStr12 =
      """
        |{"fields": [
        |      {
        |        "mode": "NULLABLE",
        |        "name": "DataVersion",
        |        "type": "NUMERIC"
        |      },
        |      {
        |        "mode": "NULLABLE",
        |        "name": "Id",
        |        "type": "STRING"
        |      },
        |      {
        |        "mode": "NULLABLE",
        |        "name": "ProcessingTime",
        |        "type": "DATETIME"
        |      },
        |      {
        |        "mode": "NULLABLE",
        |        "name": "SchemaVersion",
        |        "type": "NUMERIC"
        |      },
        |      {
        |        "mode": "NULLABLE",
        |        "name": "Source",
        |        "type": "STRING"
        |      },
        |      {
        |        "mode": "NULLABLE",
        |        "name": "CreatedAt",
        |        "type": "DATETIME"
        |      },
        |      {
        |        "fields": [
        |          {
        |            "mode": "NULLABLE",
        |            "name": "AccountId",
        |            "type": "INTEGER"
        |          },
        |          {
        |            "mode": "REPEATED",
        |            "name": "Cnpjs",
        |            "type": "STRING"
        |          }
        |        ],
        |        "mode": "NULLABLE",
        |        "name": "Data",
        |        "type": "RECORD"
        |      },
        |      {
        |        "mode": "NULLABLE",
        |        "name": "IsTracking",
        |        "type": "BOOLEAN"
        |      },
        |      {
        |        "mode": "NULLABLE",
        |        "name": "Type",
        |        "type": "STRING"
        |      }
        |    ]
        |  }
      """.stripMargin

    val expectedFinalSchemaStr23 =
      """
        |{"fields": [
        |      {
        |        "mode": "NULLABLE",
        |        "name": "Id",
        |        "type": "STRING"
        |      },
        |      {
        |        "mode": "NULLABLE",
        |        "name": "ProcessingTime",
        |        "type": "DATETIME"
        |      },
        |      {
        |        "mode": "NULLABLE",
        |        "name": "SchemaVersion",
        |        "type": "NUMERIC"
        |      },
        |      {
        |        "mode": "NULLABLE",
        |        "name": "Source",
        |        "type": "STRING"
        |      },
        |      {
        |        "mode": "NULLABLE",
        |        "name": "CreatedAt",
        |        "type": "DATETIME"
        |      },
        |      {
        |        "fields": [
        |          {
        |            "mode": "NULLABLE",
        |            "name": "AccountId",
        |            "type": "INTEGER"
        |          },
        |          {
        |            "mode": "REPEATED",
        |            "name": "Cnpjs",
        |            "type": "STRING"
        |          },
        |          {
        |            "mode": "REPEATED",
        |            "name": "ExtraField",
        |            "type": "STRING"
        |          }
        |        ],
        |        "mode": "NULLABLE",
        |        "name": "Data",
        |        "type": "RECORD"
        |      },
        |      {
        |        "mode": "NULLABLE",
        |        "name": "IsTracking",
        |        "type": "BOOLEAN"
        |      }
        |    ]
        |  }
      """.stripMargin

    val schema1 = parseSchema(event1)
    val schema2 = parseSchema(event2)
    val schema3 = parseSchema(event3)
    val expectedFinalSchema12 = parseSchema(expectedFinalSchemaStr12)
    val expectedFinalSchema23 = parseSchema(expectedFinalSchemaStr23)

    val mergedSchema12 = MergeTableSchema.createNewTableSchema(schema1, schema2)
    val mergedSchema23 = MergeTableSchema.createNewTableSchema(schema2, schema3)

    def jsonBigQuerySchemasAreEqual(schema: TableSchema, comparedSchema: TableSchema): Boolean = {
      val schemaWithSet = Json.anyToJValue(schema) transform {
        case JArray(arr) => JSet(arr.toSet)
      }
      val comparedSchemaWithSet = Json.anyToJValue(comparedSchema) transform {
        case JArray(arr) => JSet(arr.toSet)
      }
      val jsonDiff = schemaWithSet diff comparedSchemaWithSet
      jsonDiff.changed == JNothing && jsonDiff.deleted == JNothing && jsonDiff.added == JNothing
    }

    assert(jsonBigQuerySchemasAreEqual(expectedFinalSchema12, mergedSchema12))
    assert(jsonBigQuerySchemasAreEqual(expectedFinalSchema23, mergedSchema23))
  }

  "addNewFields" should "throw on wrong field type" in {
    val wrongSchema = parse(
      """
        |{
        |  "mode": "NULLABLE",
        |  "name": "Id",
        |  "type": "STRING",
        |  "fields": "potato"
        |},
      """.stripMargin).asInstanceOf[JObject]
    assertThrows[ArquiveiException](MergeTableSchema.addNewFields(List(wrongSchema), List()))
  }

}
