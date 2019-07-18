package com.arquivei.pubsubgenericeventparser

import com.arquivei.core.io.db.bigquery.JsonToTablerow
import com.arquivei.pubsubgenericeventparser.FallbackObject.FallbackRow
import com.google.api.services.bigquery.model.TableRow
import com.spotify.scio.bigquery.{BigQueryIO, BigQueryUtil}
import com.spotify.scio.io.{CustomIO, PubsubIO}
import com.spotify.scio.testing.PipelineSpec
import org.json4s._
import org.json4s.jackson.JsonMethods._

class PubsubGenericEventParserTest extends PipelineSpec {
  val yamlFile = "stream/pubsubgenericeventparser/src/test/resources/test.yaml.dist"
  implicit val formats = org.json4s.DefaultFormats



  val fallbackTableSpec = "project:arquivei_streaming_parsed_events.fallback_table"

  val inputEvents = Map(
    "eventWasSent" -> """
                        | {
                        |    "DataVersion": 2,
                        |    "Id": "111111",
                        |    "SchemaVersion": 2,
                        |    "Source": "app",
                        |    "CreatedAt": "2018-08-24 00:14:54",
                        |    "Data": {
                        |      "UserId": 1111,
                        |      "AccountId": 1111,
                        |      "StartDate": "2017-12-11 15:20:32",
                        |      "EndDate": "2017-12-29 14:19:10"
                        |    },
                        |    "IsTracking": "true",
                        |    "Type": "event-was-sent"
                        |  }
                      """.stripMargin,
    "wrongEvent" ->
      """{"sem_schema_certo":{"nome":"OI"}}"""
  )


  val in: List[String] = inputEvents.toList.map(_._2)
  val gcsout: List[String] = inputEvents.toList.map(_._2)

  val outFallback1: List[FallbackRow] = List(
    FallbackRow(
      Id = None,
      Source = None,
      Type = None,
      MessagePayload = Some("""{"SemSchemaCerto":{"Nome":"OI"}}"""),
      CreatedAt = None,
      ProcessingTime = None
    )
  )
  val outFallback2: List[FallbackRow] = List(
    FallbackRow(
      Id = Some("111111"),
      Source = Some("app"),
      Type = Some ("event-was-sent"),
      MessagePayload = Some(
        """{"DataVersion":2,"Id":"111111","SchemaVersion":2,"Source":"app","CreatedAt":"2018-08-24 00:14:54","Data":{"UserId":1111,"AccountId":1111,"StartDate":"2017-12-11 15:20:32","EndDate":"2017-12-29 14:19:10"},"IsTracking":"true","Type":"event-was-sent"}"""),
      CreatedAt = Some("2018-08-24 00:14:54"),
      ProcessingTime =  None
    )
  )

  val eventTableRow = JsonToTablerow(parse(inputEvents("eventWasSent")))
  val expectedFallbackRow = new TableRow()
    .set("Id" , "111111")
    .set("Source" , "app")
    .set("Type" , "event-was-sent")
    .set("MessagePayload" , """{"DataVersion":2,"Id":"111111","SchemaVersion":2,"Source":"app","CreatedAt":"2018-08-24 00:14:54","Data":{"UserId":1111,"AccountId":1111,"StartDate":"2017-12-11 15:20:32","EndDate":"2017-12-29 14:19:10"},"IsTracking":"true","Type":"event-was-sent"}""")
    .set("ProcessingTime" , "2018-08-24 00:14:54")
    .set("CreatedAt" , "2018-08-24 00:14:54")
  val outputTableRow = MutateBigQuerySchema.parseToFallback(eventTableRow)
  outputTableRow.remove("ProcessingTime")
  expectedFallbackRow.remove("ProcessingTime")

  "PubsubGenericEventParser to Fallback" should "parse event to fallback table" in {
    assertResult(expectedFallbackRow)(outputTableRow)
  }


  "PubsubGenericEventParser" should "work" in {
    JobTest[com.arquivei.pubsubgenericeventparser.Main.type]
      .args("run", yamlFile)
      .input(PubsubIO[String]("projects/project/subscriptions/soldera"), in)
      .output(BigQueryIO[FallbackRow](fallbackTableSpec)) (result => {
        //removes ProcessingTime before comparing (non-deterministic)
        result.map(_.copy(ProcessingTime = None)) should containInAnyOrder(outFallback1)
      })
      .output(BigQueryIO[FallbackRow]("fallback:fallback.fallback")) (result => {
        //removes ProcessingTime before comparing (non-deterministic)
        result.map(_.copy(ProcessingTime = None)) should containInAnyOrder(outFallback2)
      })
      .run()
  }


  val schema1 = BigQueryUtil.parseSchema(
    """
      |{"fields": [
      |      {
      |        "mode": "NULLABLE",
      |        "name": "test1",
      |        "type": "NUMERIC"
      |      },
      |      {
      |        "mode": "NULLABLE",
      |        "name": "test2",
      |        "type": "STRING"
      |      }
      |]}
    """.stripMargin
  )
  val schema1_mod = BigQueryUtil.parseSchema(
    """
      |{"fields": [
      |      {
      |        "mode": "NULLABLE",
      |        "name": "test2",
      |        "type": "STRING"
      |      },
      |      {
      |        "mode": "NULLABLE",
      |        "name": "test1",
      |        "type": "NUMERIC"
      |      }
      |]}
    """.stripMargin
  )
  val schema2 = BigQueryUtil.parseSchema(
    """
      |{"fields": [
      |      {
      |        "mode": "NULLABLE",
      |        "name": "test2",
      |        "type": "STRING"
      |      },
      |      {
      |        "mode": "NULLABLE",
      |        "name": "test1",
      |        "type": "NUMERIC"
      |      },
      |      {
      |        "mode": "NULLABLE",
      |        "name": "test1",
      |        "type": "RECORD",
      |        "fields" : [
      |        {
        |        "mode": "NULLABLE",
        |        "name": "test3",
        |        "type": "STRING"
        |      },
        |      {
        |        "mode": "NULLABLE",
        |        "name": "test4",
        |        "type": "NUMERIC"
        |      }
      |        ]
      |      }
      |]}
    """.stripMargin
  )
  val schema2_mod = BigQueryUtil.parseSchema(
    """
      |{"fields": [
      |       {
      |        "mode": "NULLABLE",
      |        "name": "test1",
      |        "type": "RECORD",
      |        "fields" : [
        |      {
        |        "mode": "NULLABLE",
        |        "name": "test4",
        |        "type": "NUMERIC"
        |      },
        |      {
        |        "mode": "NULLABLE",
        |        "name": "test3",
        |        "type": "STRING"
      |         }
      |        ]
      |      },
      |      {
      |        "mode": "NULLABLE",
      |        "name": "test2",
      |        "type": "STRING"
      |      },
      |      {
      |        "mode": "NULLABLE",
      |        "name": "test1",
      |        "type": "NUMERIC"
      |      }
      |]}
    """.stripMargin
  )
  val schema3 = BigQueryUtil.parseSchema(
    """
      |{"fields": [
      |       {
      |        "mode": "NULLABLE",
      |        "name": "test1",
      |        "type": "RECORD",
      |        "fields" : [
      |      {
      |        "mode": "NULLABLE",
      |        "name": "test4",
      |        "type": "NUMERIC"
      |      }
      |        ]
      |      },
      |      {
      |        "mode": "NULLABLE",
      |        "name": "test2",
      |        "type": "STRING"
      |      },
      |      {
      |        "mode": "NULLABLE",
      |        "name": "test1",
      |        "type": "NUMERIC"
      |      }
      |]}
    """.stripMargin
  )
  "Pubsub - MutateBigQuerySchema functions" should "work" in {
    assert(MutateBigQuerySchema.jsonBigQuerySchemasAreEqual(schema1, schema1_mod))
    assert(MutateBigQuerySchema.jsonBigQuerySchemasAreEqual(schema2, schema2_mod))
    assertResult(false)(MutateBigQuerySchema.jsonBigQuerySchemasAreEqual(schema2_mod, schema3))
  }


}
