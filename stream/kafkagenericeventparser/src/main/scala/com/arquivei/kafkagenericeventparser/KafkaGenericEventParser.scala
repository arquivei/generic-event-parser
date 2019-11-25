package com.arquivei.kafkagenericeventparser


import java.time.Instant

import com.arquivei.core.configs.Options
import com.arquivei.core.dateparser.DateParser.{arquiveiDateFormat, convertToSaoPauloOrString}
import com.arquivei.core.io.db.bigquery.{JsonToTablerow, SchemaConverter, SchemaMigrator, TableFullName}
import com.arquivei.core.workflow.StreamPipeline
import com.arquivei.kafkagenericeventparser.Coders._
import com.arquivei.kafkagenericeventparser.StreamingInsert.{WriteToBigQuery, WriteToBigQueryAfterSchemaMutation}
import com.google.api.services.bigquery.model.TableRow
import com.google.gson.Gson
import com.spotify.scio.ScioContext
import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.bigquery.BigQuerySysProps
import com.spotify.scio.bigquery._
import com.spotify.scio.values.{SCollection, WindowOptions}
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{CreateDisposition, WriteDisposition}
import org.apache.beam.sdk.transforms.{Reshuffle, Values}
import org.apache.beam.sdk.transforms.windowing.{AfterProcessingTime, AfterWatermark}
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.joda.time.Duration
import org.json4s.JsonAST.JObject
import org.json4s._
import org.json4s.jackson.JsonMethods.parse
import org.slf4j.LoggerFactory

import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}


class KafkaGenericEventParser(sc: ScioContext) extends StreamPipeline[SCollection[KV[String, String]]] {
  override def migrate(): Unit = {
    SchemaMigrator(
      Options().gcpProject,
      Options().getOptionString("datasetId"),
      Options().getOptionString("fallbackTableId"),
      SchemaConverter(BigQueryType.schemaOf[FallbackObject.FallbackRow]),
      isPartitioned = true
    ).migrate()
  }

  override def build(start: SCollection[KV[String, String]]): Unit = {
    val projectId = Options().gcpProject
    val datasetId = Options().getOptionString("datasetId")
    val fallbackTableId = Options().getOptionString("fallbackTableId")
    val genericEventParserFunctions = GenericEventParserFunctions(projectId, datasetId, fallbackTableId)

    val windowDurationMinutes = Options().getOptionInt("windowDurationMinutes")
    val windowFiringMinutes = Options().getOptionInt("windowFiringDelayMinutes")

    System.setProperty(BigQuerySysProps.CacheEnabled.flag, "false")

    val parsedJValue =
      start
      .withName("To Value").applyTransform(Values.create())
      .applyTransform(Reshuffle.viaRandomKey())
      .withName("Parse String to Option[JValue]").map(genericEventParserFunctions.parseMessage)
      .withName("From Options to JValues").flatMap(element => element)

    val nonFilteredPipeline =
      parsedJValue
        .withName("Mutate Datetime Strings").map(genericEventParserFunctions.mutateDateStringsToSaoPauloTZMapper)
        .withName("Snakize Keys").map(input => input.snakizeKeys)
        .withName("Pascalize Keys").map(input => input.pascalizeKeys)
        .withName("Mutate to valid BigQuery Field Name").map(genericEventParserFunctions.mutateFieldNameToValid)
        .withName("JValue To TableRow").map(JsonToTablerow.apply)
        .withName("Append ProcessingTime").map(genericEventParserFunctions.appendProcessingTime)

    nonFilteredPipeline
      .withName("Filter in FallbackTable").filter(genericEventParserFunctions.tableRowIsFallback)
      .withName("Parse to FallbackTable").map(genericEventParserFunctions.parseToFallback)
      .withName("Insert Failed to FallbackTable").saveAsTypedBigQuery(
      genericEventParserFunctions.fallbackTableSpec,
      writeDisposition = WriteDisposition.WRITE_APPEND,
      createDisposition = CreateDisposition.CREATE_NEVER
    )


    val onlyValidEvents = nonFilteredPipeline
      .withName("Filter off FallbackTable").filter(
        inputTableRow => !genericEventParserFunctions.tableRowIsFallback(inputTableRow)
      )

    val bigQueryFailures = if (sc.isTest){
       onlyValidEvents
    }  else {
       onlyValidEvents.withName("Save To BQ").applyTransform(
         new WriteToBigQuery(genericEventParserFunctions.getTableSpec)
       )
    }

    bigQueryFailures
    .withName("MutateSchema for Failures").applyTransform(
      new MutateBigQuerySchema(
        genericEventParserFunctions.getTableSpec,
        genericEventParserFunctions.fallbackTableSpec,
        sc.isTest
      )
    )
    .withName("Remove ProcessingTime For Deduplication").map(genericEventParserFunctions.removeProcessingTime)
    .withName("Map to Tuple With Id For Deduplicating Events").map(
      input =>
        (
          input.getOrDefault("Id", "NoId").asInstanceOf[String],
          input
        )
    )
    .withName("Apply Session Window Based on Id").withSessionWindows(
      gapDuration = Duration.standardMinutes(windowDurationMinutes),
      options = WindowOptions(
        allowedLateness = Duration.ZERO,
        trigger = AfterWatermark.pastEndOfWindow()
          .withEarlyFirings(
            AfterProcessingTime
              .pastFirstElementInPane()
              .plusDelayOf(Duration.standardMinutes(windowFiringMinutes))
          ),
        accumulationMode = AccumulationMode.DISCARDING_FIRED_PANES
      )
    )
    .withName("Group By Id").groupBy(input => input._1)
    .withName("Deduplicate Events").map({
          input => {
            val inputListDistinct = input._2.toList.distinct
            inputListDistinct.map(_._2)
          }
        })
    .withName("From List of TableRow to TableRow - After Mutation").flatMap(element => element)
    .withName("Append ProcessingTime after ").map(genericEventParserFunctions.appendProcessingTime)
    .withName("Insert After Schema Mutation").applyTransform(
      new WriteToBigQueryAfterSchemaMutation(genericEventParserFunctions.getTableSpec, sc.isTest)
    )
    .withName("From Options to Values - After Second Insertion").flatMap(element => element)
    .withName("Parse to FallbackTable After Mutation").map(genericEventParserFunctions.parseToFallback)
    .withName("Insert Failed to FallbackTable After Mutation")
    .withGlobalWindow().saveAsTypedBigQuery(
      {if (sc.isTest) "project:dataset.fallback" else genericEventParserFunctions.fallbackTableSpec},
      writeDisposition = WriteDisposition.WRITE_APPEND,
      createDisposition = CreateDisposition.CREATE_NEVER
    )

  }

}

object FallbackObject {
  @BigQueryType.toTable
  case class FallbackRow
  (
    Id: Option[String],
    Source: Option[String],
    Type: Option[String],
    MessagePayload: Option[String],
    CreatedAt: Option[String],
    ProcessingTime: Option[String]
  )
}


case class GenericEventParserFunctions (projectId: String, datasetId: String, fallbackTableId: String) {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  val fallbackSource = "without_source"
  val fallbackTableSpec = TableFullName(
    projectId,
    datasetId,
    fallbackTableId
  )
  val illegalChars = new Regex("""[^a-zA-Z0-9\_]+""")

  def appendProcessingTime(inputRow: TableRow): TableRow = {
    val processingTime = Instant.now()
    val processingTimeStr = arquiveiDateFormat.format(processingTime)
    val outputTableRow = inputRow.clone()
    outputTableRow.set("ProcessingTime", processingTimeStr)
  }

  def removeProcessingTime(inputRow: TableRow): TableRow = {
    val outputTableRow = inputRow.clone()
    outputTableRow.remove("ProcessingTime")
    outputTableRow
  }

  def parseMessage(msg: String): Option[JValue] = {
    implicit val formats = org.json4s.DefaultFormats
    Try(parse(msg)) match {
      case Success(obj) => {
        obj match {
          case _: JObject => Some(obj)
          case _ => {
            LOG.error(s"Problem parsing message! Not a valid JSON: $obj")
            None
          }
        }
      }
      case Failure(_) =>
        LOG.error(s"Problem parsing message! It may not be a valid message: $msg")
        None
    }
  }

  def parseToFallback(inputRow: TableRow): FallbackObject.FallbackRow = {

    implicit val formats = org.json4s.DefaultFormats

    val outputTableRow = inputRow.clone()
    outputTableRow.remove("ProcessingTime")

    val jsonStr = new Gson().toJson(outputTableRow)

    val payloadObj = parseMessage(jsonStr).getOrElse(JObject())

    val processingTime = Instant.now()
    val processingTimeStr = arquiveiDateFormat.format(processingTime)

    FallbackObject.FallbackRow(
      Id = (payloadObj \ "Id").extractOpt[String],
      Source = (payloadObj \ "Source").extractOpt[String],
      Type = (payloadObj \ "Type").extractOpt[String],
      MessagePayload = Some(jsonStr),
      CreatedAt = (payloadObj \ "CreatedAt").extractOpt[String],
      ProcessingTime = Some(processingTimeStr)
    )
  }


  def tableRowIsFallback (input: TableRow): Boolean = {
    getTableSpec(input) == fallbackTableSpec
  }


  def getTableSpec(input: TableRow): String = {

    val eventNameRaw = input.getOrDefault("Type", fallbackTableId).toString
    val eventName = illegalChars.replaceAllIn(eventNameRaw, "_").toLowerCase
    val sourceNameRaw = input.getOrDefault("Source", fallbackSource).toString
    val sourceName = illegalChars.replaceAllIn(sourceNameRaw, "_").toLowerCase
    if (eventName == fallbackTableId) {
      fallbackTableSpec
    } else {
      TableFullName(projectId, datasetId, s"${sourceName}_$eventName")
    }
  }

  def mutateDateStringsToSaoPauloTZMapper(input: JValue): JValue = {
    implicit val format = org.json4s.DefaultFormats
    input transform {
      case JString(inputStr) => JString(convertToSaoPauloOrString(inputStr))
      case jArray: JArray => jArray transform {
        case JString(inputStr) => JString(convertToSaoPauloOrString(inputStr))
      }
    }
  }

  def mutateFieldNameToValid(input: JValue): JValue = {
    implicit val format = org.json4s.DefaultFormats
    val illegalField = new Regex("""^[0-9]|[^a-zA-Z0-9\_]""")
    input transformField {
      case (x: String, y: JValue) if illegalField.findAllIn(x).nonEmpty => {
        val newField = illegalField.replaceAllIn(x, "_")
        (newField, y)
      }
    }
  }

}
