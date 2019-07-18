package com.arquivei.core.io.db.bigquery

import com.arquivei.core.dateparser.DateParser._
import com.arquivei.core.exceptions.ArquiveiException
import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import org.json4s.JsonAST.JValue
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.collection.JavaConverters._
import scala.util.matching.Regex

object JValueToTableSchema {


  val idFieldRegex = new Regex("""Id$""")

  implicit val formats = org.json4s.DefaultFormats

  def apply(inputRow: String): TableSchema = {
    val parsedRow = try parse(inputRow)
    catch {
      case _: Throwable => throw new ArquiveiException(s"Error Parsing Json When Discovering Schema: $inputRow")
    }
    val tableSchema = getTableSchema(parsedRow)
    tableSchema
  }

  def getTableSchema(inputRow: JValue): TableSchema = {

    val jValueMap = inputRow.noNulls.extract[Map[String, JValue]]
    new TableSchema().setFields(jValueMap.map(
      {
        case (fieldName, fieldValue) =>
          convert(fieldName, fieldValue, "NULLABLE")
      }).toList.asJava
    )
  }

  def isIdField(inputStr: String): Boolean = {
    idFieldRegex.findFirstMatchIn(inputStr).isDefined
  }


  def convert(fieldName: String, fieldValue: JValue, mode: String): TableFieldSchema = {

    def fieldSchema(typeStr: String, modeStr: String = mode, fieldNameStr: String = fieldName): TableFieldSchema =
      new TableFieldSchema().setName(fieldNameStr).setMode(modeStr).setType(typeStr)

    fieldValue match {
      case JBool(_) => fieldSchema("BOOLEAN")
      case JInt(_) => if (isIdField(fieldName)) fieldSchema("INTEGER") else fieldSchema("NUMERIC")
      case JLong(_) => fieldSchema("NUMERIC")
      case JDecimal(_) => fieldSchema("NUMERIC")
      case JDouble(_) => fieldSchema("NUMERIC")
      case JString(inputString) => stringToTableSchema(fieldName, inputString)
      case JObject(listOfJFields) => jObjectToTableSchema(fieldName, listOfJFields, mode)
      case JArray(listOfJValue) => convertJArray(fieldName, listOfJValue)
      case _ => throw new ArquiveiException("InvalidField")
    }
  }

  def jObjectToTableSchema(fieldName: String, listOfJFields: List[JField], mode: String): TableFieldSchema = {
    new TableFieldSchema().setName(fieldName).setMode(mode).setType("RECORD").setFields(
      listOfJFields.map(
        {
          case (name, value) =>
            convert(name, value, "NULLABLE")
        }).asJava
    )
  }

  def stringToTableSchema(fieldName: String, inputString: String, mode: String = "NULLABLE"): TableFieldSchema = {
    def fieldSchema(typeStr: String, modeStr: String = mode, fieldNameStr: String = fieldName): TableFieldSchema =
      new TableFieldSchema().setName(fieldNameStr).setMode(modeStr).setType(typeStr)

      if (isEventDatetime(inputString)) {
      fieldSchema("DATETIME")
      } else if (isEventDate(inputString)) {
        fieldSchema("DATE")
      } else {
        fieldSchema("STRING")
      }
  }


  def convertJArray(fieldName: String, listOfJValue: List[JValue]): TableFieldSchema = {

    def fieldSchema(typeStr: String, modeStr: String, fieldNameStr: String = fieldName): TableFieldSchema =
      new TableFieldSchema().setName(fieldNameStr).setMode(modeStr).setType(typeStr)

    listOfJValue match {
      case List(_: JInt, _*) =>
        if (isIdField(fieldName)) {
          fieldSchema("INTEGER", modeStr = "REPEATED")
        } else {
          fieldSchema("NUMERIC", modeStr = "REPEATED")
        }
      case List(_: JBool, _*) => fieldSchema("BOOLEAN", modeStr = "REPEATED")
      case List(_: JLong, _*) => fieldSchema("NUMERIC", modeStr = "REPEATED")
      case List(_: JDecimal, _*) => fieldSchema("NUMERIC", modeStr = "REPEATED")
      case List(_: JDouble, _*) => fieldSchema("NUMERIC", modeStr = "REPEATED")
      case List(_: JString, _*) =>
        if (!listOfJValue.forall({ inputJString => isEventDatetime(inputJString.extract[String]) })) {
          if (!listOfJValue.forall({ inputJString => isEventDate(inputJString.extract[String]) })) {
            fieldSchema("STRING", modeStr = "REPEATED")
          } else {
            fieldSchema("DATE", modeStr = "REPEATED")
          }
        } else {
          fieldSchema("DATETIME", modeStr = "REPEATED")
        }
      case List(_: JArray, _*) => throw new ArquiveiException("BigQuery will not Accept Array of Array")
      case List(_: JObject, _*) =>
        val tableSchema = listOfJValue.foldLeft(new TableSchema())(
          {
            (accumulatedTableSchema: TableSchema, iteratedJField: JValue) =>
              val iteratedTableSchema = iteratedJField match {
                case JObject(listOfJFields) => new TableSchema().setFields(
                  listOfJFields.map(
                    {
                      case (name, value) =>
                        convert(name, value, "NULLABLE")
                    }).asJava
                )
                case _ => throw new ArquiveiException("All elements in this array must be of the same type (JObject)")
              }
              MergeTableSchema.createNewTableSchema(accumulatedTableSchema, iteratedTableSchema)
          })
        new TableFieldSchema().setName(fieldName).setMode("REPEATED").setType("RECORD").setFields(tableSchema.getFields)
    }
  }



}
