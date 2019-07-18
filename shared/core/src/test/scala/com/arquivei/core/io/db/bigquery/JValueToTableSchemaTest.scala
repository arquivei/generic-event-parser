package com.arquivei.core.io.db.bigquery

import java.io.StringReader

import com.arquivei.core.exceptions.ArquiveiException
import com.arquivei.core.formats.Json
import com.google.api.client.json.JsonObjectParser
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.bigquery.model.TableSchema
import org.json4s.JsonAST._
import org.scalatest.FlatSpec

class JValueToTableSchemaTest extends FlatSpec {

  "JValueToTableSchema" should "infer BigQuery's Schema from Event Data" in {

    lazy val jsonObjectParser = new JsonObjectParser(new JacksonFactory)

    /** Parse a schema string. */
    def parseSchema(schemaString: String): TableSchema =
      jsonObjectParser
        .parseAndClose(new StringReader(schemaString), classOf[TableSchema])

    implicit val format = org.json4s.DefaultFormats

    val eventWasSentEvent =
      """
        |{
        |    "DataVersion": 3,
        |    "ProcessingTime": "2018-06-04 07:44:25",
        |    "Id": "01CF56BSH8QFKR9QFGMSN5NNY0",
        |    "SchemaVersion": 1,
        |    "Source": "system",
        |    "CreatedAt": "2018-06-04 07:44:25",
        |    "Data": {
        |      "Type": "1",
        |      "CompanyId": 125125,
        |      "CreatedAt": "2018-06-04 07:44:25",
        |      "Value": "346346346346346.00"
        |    },
        |    "Type": "an-event-was-sent"
        |  }
      """.stripMargin

    val eventWasSentExpectedSchemaStr =
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
        |            "name": "Type",
        |            "type": "STRING"
        |          },
        |          {
        |            "mode": "NULLABLE",
        |            "name": "CompanyId",
        |            "type": "INTEGER"
        |          },
        |          {
        |            "mode": "NULLABLE",
        |            "name": "CreatedAt",
        |            "type": "DATETIME"
        |          },
        |          {
        |            "mode": "NULLABLE",
        |            "name": "Value",
        |            "type": "STRING"
        |          }
        |        ],
        |        "mode": "NULLABLE",
        |        "name": "Data",
        |        "type": "RECORD"
        |      },
        |      {
        |        "mode": "NULLABLE",
        |        "name": "Type",
        |        "type": "STRING"
        |      }
        |    ]
        |  }
      """.stripMargin




    val genericEvent =
      """
      |{
      |    "Double": 121910512905.12412,
      |    "Date": "2018-01-01",
      |    "RepeatedInt": [1,2,3],
      |    "RepeatedDouble": [1.1,2.2,3.3],
      |    "RepeatedString": ["OI","tCHAU"],
      |    "RepeatedDatetime": ["2017-08-04 14:02:23", "2017-08-04T12:02:23-01:00"],
      |    "RepeatedDate": ["2018-01-01", "2020-01-01"],
      |    "RepeatedId": [1,2,3],
      |    "RepeatedBoolean": [true,false,false]
      |}
      """.stripMargin

    val genericSchema =
      """
        |{"fields": [
        |      {
        |        "mode": "NULLABLE",
        |        "name": "Double",
        |        "type": "NUMERIC"
        |      },
        |      {
        |        "mode": "NULLABLE",
        |        "name": "Date",
        |        "type": "DATE"
        |      },
        |      {
        |        "mode": "REPEATED",
        |        "name": "RepeatedInt",
        |        "type": "NUMERIC"
        |      },
        |      {
        |        "mode": "REPEATED",
        |        "name": "RepeatedDouble",
        |        "type": "NUMERIC"
        |      },
        |      {
        |        "mode": "REPEATED",
        |        "name": "RepeatedString",
        |        "type": "STRING"
        |      },
        |      {
        |        "mode": "REPEATED",
        |        "name": "RepeatedDatetime",
        |        "type": "DATETIME"
        |      },
        |      {
        |        "mode": "REPEATED",
        |        "name": "RepeatedDate",
        |        "type": "DATE"
        |      },
        |      {
        |        "mode": "REPEATED",
        |        "name": "RepeatedId",
        |        "type": "INTEGER"
        |      },
        |      {
        |        "mode": "REPEATED",
        |        "name": "RepeatedBoolean",
        |        "type": "BOOLEAN"
        |      }
        |]}
      """.stripMargin


    val eventWasSentExpectedSchema = parseSchema(eventWasSentExpectedSchemaStr)
    val genericExpectedSchema = parseSchema(genericSchema)

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


    assert(jsonBigQuerySchemasAreEqual(eventWasSentExpectedSchema, JValueToTableSchema(eventWasSentEvent)))
    assert(jsonBigQuerySchemasAreEqual(genericExpectedSchema, JValueToTableSchema(genericEvent)))

  }

  it should "throw  on unparseable input" in {
    assertThrows[ArquiveiException](JValueToTableSchema("unparseable"))
  }

  "convert" should "handle scalars" in {
    val table = List(
      ("fieldName", JBool(true), "NULLABLE", "BOOLEAN"),
      ("intField", JInt(15), "NULLABLE", "NUMERIC"),
      ("intFieldId", JInt(15), "NULLABLE", "INTEGER"),
      ("longField", JLong(100), "REQUIRED", "NUMERIC"),
      ("decimal", JDecimal(15), "REPEATED", "NUMERIC"),
      ("double", JDouble(1.5), "NULLABLE", "NUMERIC")
    )

    for ((fieldName, jvalue, mode, fieldType) <- table) {
      val result = JValueToTableSchema.convert(fieldName, jvalue, mode)
      assertResult(fieldName)(result.getName)
      assertResult(mode)(result.getMode)
      assertResult(fieldType)(result.getType)
    }

    assertThrows[ArquiveiException](JValueToTableSchema.convert("ETC", JSet(Set.empty), "NULLABLE"))
  }

  "convertJArray" should "handle scalar arays" in {
    val table = List(
      ("intField", List(JInt(10)), "NUMERIC"),
      ("boolField", List(JBool(false)), "BOOLEAN"),
      ("longField", List(JLong(15)), "NUMERIC"),
      ("decimalField", List(JDecimal(100)), "NUMERIC"),
      ("stringField", List(JString("kartoffel")), "STRING")
    )

    for ((fieldName, jvalue, fieldType) <- table) {
      val result = JValueToTableSchema.convertJArray(fieldName, jvalue)
      assertResult(fieldName)(result.getName)
      assertResult("REPEATED")(result.getMode)
      assertResult(fieldType, fieldName)(result.getType)
    }
  }

  it should "throw on array inside array" in {
    assertThrows[ArquiveiException](JValueToTableSchema.convertJArray("ETC", List(JArray(List()))))
  }

  it should "throw on object array of different types" in {
    assertThrows[ArquiveiException](JValueToTableSchema.convertJArray("ETC", List(
      JObject("a"-> JString("b")),
      JString("outsider")
    )))
  }
}
