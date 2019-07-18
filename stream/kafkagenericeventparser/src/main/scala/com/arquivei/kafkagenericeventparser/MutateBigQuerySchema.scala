package com.arquivei.kafkagenericeventparser

import java.time.Instant

import com.arquivei.core.dateparser.DateParser.arquiveiDateFormat
import com.arquivei.core.formats.Json
import com.arquivei.core.io.db.bigquery._
import com.google.api.services.bigquery.model.{TableRow, TableSchema}
import com.google.gson.Gson
import com.spotify.scio.bigquery.BigQuerySysProps
import com.spotify.scio.bigquery.client._
import org.apache.beam.sdk.coders.VarIntCoder
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{CreateDisposition, WriteDisposition}
import org.apache.beam.sdk.state.{StateSpecs, ValueState}
import org.apache.beam.sdk.transforms.DoFn.{ProcessElement, StateId}
import org.apache.beam.sdk.transforms.{DoFn, PTransform, ParDo}
import org.apache.beam.sdk.values.{KV, PCollection}
import org.json4s.JsonAST.{JArray, JNothing, JObject, JSet}
import org.json4s.jackson.JsonMethods.parse
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}



class MutateBigQuerySchema(getTableSpec: TableRow => String, fallbackTableSpec: String, isTest: Boolean)
  extends PTransform[PCollection[TableRow], PCollection[TableRow]] {


  override def expand(input: PCollection[TableRow]): PCollection[TableRow] = {
    input
      .apply("Gen KeyPair", ParDo.of(new KvPair))
      .apply("Mutate BigQuerySchema",  ParDo.of(new MutateBigQuerySchemaDoFn))
  }

  class KvPair extends DoFn[TableRow, KV[String, TableRow]] {
    @ProcessElement def processElement(c: DoFn[TableRow, KV[String, TableRow]]#ProcessContext): Unit = {
      c.output(KV.of(getTableSpec(c.element), c.element))
    }
  }

  class MutateBigQuerySchemaDoFn extends DoFn[KV[String,TableRow], TableRow] {

    private val LOG = LoggerFactory.getLogger(this.getClass)

    @StateId("count") private val countState = StateSpecs.value(VarIntCoder.of())

    @ProcessElement
    def processElement(c: DoFn[KV[String,TableRow], TableRow] #ProcessContext, @StateId("count") counter: ValueState[Integer]): Unit = {
      val inputElement = c.element()
      if (isTest) {
        c.output(inputElement.getValue)
      } else {
        if (mutateSchema(inputElement, counter)) {
          c.output(inputElement.getValue)
        }

      }
    }


    def mutateSchema(inputElement: KV[String, TableRow], counter: ValueState[Integer]): Boolean = {
      val currentCounter = counter.read() match {
        case null => new Integer(0)
        case value: Integer => new Integer(value)
      }
      val tableSpec = inputElement.getKey
      val tableReference = BigQueryHelpers.parseTableSpec(tableSpec)
      val inputRow = inputElement.getValue
      val inputJson = new Gson().toJson(inputRow)
      val inputSchema = JValueToTableSchema(inputJson)

      val bq = BigQuery(tableReference.getProjectId)
      System.setProperty(BigQuerySysProps.CacheEnabled.flag, "false")

      val mutationResult = if(bq.tables.exists(tableSpec)) {
        Try({
          val tableSchema = bq.tables.schema(tableSpec)
          val newSchema = MergeTableSchema.createNewTableSchema(tableSchema, inputSchema)

          if (!MutateBigQuerySchema.jsonBigQuerySchemasAreEqual(newSchema, tableSchema)) {
            LOG.info(s"Schema will mutate:\nNewSchema: $newSchema\n\nOldSchema: $tableSchema")
            SchemaMigrator(
              project = tableReference.getProjectId,
              dataset = tableReference.getDatasetId,
              tableName = tableReference.getTableId,
              schema = SchemaConverter(newSchema)
            ).migrate()
          }
        })
      } else {
        Try({
          LOG.info("Table will be created inside schema mutation DoFn.")
          SchemaMigrator(
            project = tableReference.getProjectId,
            dataset = tableReference.getDatasetId,
            tableName = tableReference.getTableId,
            schema = SchemaConverter(inputSchema)
          ).migrate()
          LOG.warn("Table was created inside schema mutation DoFn.")
        })
      }


      mutationResult match {
        case Success(_) => {
          counter.write(currentCounter + new Integer(1))
          true
        }
        case Failure(_) => {
          LOG.error(s"Schema Mutation Failed. Inserting $inputRow in fallbackTable")
          val insertTry = Try(bq.tables.writeRows(
            fallbackTableSpec,
            List(MutateBigQuerySchema.parseToFallback(inputRow)),
            null,
            WriteDisposition.WRITE_APPEND,
            CreateDisposition.CREATE_NEVER))
          insertTry match {
            case Success(_) => {
              counter.write(currentCounter + new Integer(1))
            }
            case Failure(_) => {
              counter.write(currentCounter + new Integer(1))
              LOG.error(s"Failed inserting $inputRow to fallbackTable in SchemaMutate Stage")
            }
          }
          false
        }
      }
    }
  }
}

object MutateBigQuerySchema {

  private val LOG = LoggerFactory.getLogger(this.getClass)

  def parseToFallback(inputRow: TableRow): TableRow = {

    implicit val formats = org.json4s.DefaultFormats
    inputRow.remove("ProcessingTime")

    val jsonStr = new Gson().toJson(inputRow)

    val payloadObj = Try(parse(jsonStr)) match {
      case Success(obj) => obj
      case Failure(_) =>
        LOG.warn(s"Problem parsing message! It may not be a valid message: $jsonStr")
        JObject()
    }

    val processingTime = Instant.now()
    val processingTimeStr = arquiveiDateFormat.format(processingTime)

    val output = new TableRow()
      .set("Id", (payloadObj \ "Id").extractOrElse[String]("WithoutId"))
      .set("Source", (payloadObj \ "Source").extractOrElse[String]("WithoutSource"))
      .set("Type", (payloadObj \ "Type").extractOrElse[String]("WithoutType"))
      .set("MessagePayload", jsonStr)
      .set("CreatedAt", (payloadObj \ "CreatedAt").extractOrElse[String]("WithoutCreatedAt"))
      .set("ProcessingTime", processingTimeStr)
    output
  }


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



}
