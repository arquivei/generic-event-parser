package com.arquivei.core.io.db.bigquery

import java.io.StringReader

import com.arquivei.core.exceptions.ArquiveiException
import com.arquivei.core.formats.Json
import com.google.api.client.json.JsonObjectParser
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.bigquery.model.TableSchema
import org.json4s.JField
import org.json4s.JsonAST.{JArray, JNothing, JObject}
import org.json4s.jackson.JsonMethods.compact
import org.slf4j.LoggerFactory

object MergeTableSchema {

  private val LOG = LoggerFactory.getLogger(this.getClass)

  val BIGQUERY_FIELDS = "fields"
  val BIGQUERY_FIELD_NAME = "name"

  def createNewTableSchema(oldTableSchema: TableSchema, actualEventSchema: TableSchema): TableSchema = {
    implicit val format = org.json4s.DefaultFormats

    val newSchema = JObject(
      List(JField(BIGQUERY_FIELDS,
        JArray(
          addNewFields(
            fieldsToBeAdded = (Json.anyToJValue(actualEventSchema) \ BIGQUERY_FIELDS).extract[List[JObject]],
            initialFields = (Json.anyToJValue(oldTableSchema) \ BIGQUERY_FIELDS).extract[List[JObject]]
          ).toList
        )
      ))
    )

    lazy val jsonObjectParser = new JsonObjectParser(new JacksonFactory)

    /** Parse a schema string. */
    def parseSchema(schemaString: String): TableSchema =
      jsonObjectParser
        .parseAndClose(new StringReader(schemaString), classOf[TableSchema])

    parseSchema(compact(newSchema))
  }

  def notInTheFields(field: JObject, target: Seq[JObject]): Boolean = {
    !target.exists {
      case `field` => true
      case _ => false
    }
  }

  def hasRecord(recordName: String, target: Seq[JObject]): Boolean = {
    implicit val format = org.json4s.DefaultFormats
    target.exists(
      jObject => {
        (jObject \ BIGQUERY_FIELD_NAME).extract[String] == recordName
      }
    )
  }

  def getRecordFields(recordName: String, target: Seq[JObject]): Seq[JObject] = {
    implicit val format = org.json4s.DefaultFormats
    (target.filter(
      jObject => {
        (jObject \ BIGQUERY_FIELD_NAME).extract[String] == recordName
      }
    ).head \ BIGQUERY_FIELDS).extract[List[JObject]]
  }


  def addNewFields(fieldsToBeAdded: Seq[JObject], initialFields: Seq[JObject]): Seq[JObject] = {
    implicit val format = org.json4s.DefaultFormats
    fieldsToBeAdded.foldLeft(initialFields)(
      (finalFields: Seq[JObject], newJObjectField: JObject) => {
        val newChildrenFields = newJObjectField \ BIGQUERY_FIELDS
        newChildrenFields match {
          case JNothing =>
            if (notInTheFields(newJObjectField, finalFields)) {
              finalFields ++ Seq(newJObjectField)
            } else {
              finalFields
            }
          case JArray(_) => addChildrenFieldsToSchema(newJObjectField, finalFields)
          case _ => throw new
              ArquiveiException(s"new Fields to be added are not JArray or JNothing," +
                s" should not be happening: \nfields to be added: $fieldsToBeAdded\ninitialFields: $initialFields")
        }
      }
    )
  }

  def addChildrenFieldsToSchema(newJObjectField: JObject, finalFields: Seq[JObject]): Seq[JObject] = {
    implicit val format = org.json4s.DefaultFormats
    val newFieldName = (newJObjectField \ BIGQUERY_FIELD_NAME).extract[String]
    if (hasRecord(newFieldName, finalFields)) {
      val childrenFieldList = (newJObjectField \ BIGQUERY_FIELDS).extract[List[JObject]]
      val recordFields = getRecordFields(newFieldName, finalFields)
      val fieldsToBeAdded = childrenFieldList.filter(
        jObject => notInTheFields(jObject, recordFields)
      )
      val newChildrenFields = addNewFields(fieldsToBeAdded, recordFields)
      finalFields.map(jObject => transformMutatedChild(jObject, newFieldName, newChildrenFields))
    } else {
      finalFields ++ Seq(newJObjectField)
    }
  }

  def transformMutatedChild(inputJObject: JObject, newFieldName: String, newChildrenFields: Seq[JObject]): JObject = {
    implicit val format = org.json4s.DefaultFormats
    val inputJObjectIsTheNewField = (inputJObject \ BIGQUERY_FIELD_NAME).extract[String] == newFieldName
    (inputJObject transformField {
      case (BIGQUERY_FIELDS, _) if inputJObjectIsTheNewField => (BIGQUERY_FIELDS, JArray(newChildrenFields.toList))
    }).asInstanceOf[JObject]
  }


}
