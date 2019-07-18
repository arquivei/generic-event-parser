package com.arquivei.core.io.db.bigquery

import java.io.IOException

import com.arquivei.core.exceptions.ArquiveiException
import com.google.api.services.bigquery.model.TableRow
import org.json4s.jackson.JsonMethods._
import org.json4s._

import scala.collection.JavaConverters._


object JsonToTablerow {
  def apply(input: JObject): TableRow = {
    implicit val format = org.json4s.DefaultFormats
    val sanitizedInput = input.noNulls remove {
      case JArray(listOf) => listOf match {
        case Nil => true
        case _ => false
      }
      case JObject(Nil) => true
      case _ => false
    }
    sanitizedInput match {
      case JNothing => new TableRow()
      case _ => convert(sanitizedInput.noNulls).asInstanceOf[TableRow]
    }

  }

  def apply(input: JValue): TableRow = {
    apply(input.asInstanceOf[JObject])
  }
  // scalastyle:off cyclomatic.complexity
  private def convert(value: JValue): AnyRef = {
    def fail() = throw new ArquiveiException(s"Failed To Convert From JObject to TableRow: $value")

    value match {
      case JNothing => fail()
      case JNull => fail()
      case JString(x) => new java.lang.String(x)
      case JDouble(x) => new java.lang.Double(x)
      case JDecimal(x) => new java.lang.Double(x.doubleValue())
      case JLong(x) => new java.lang.Long(x)
      case JInt(x) => new java.lang.Long(x.longValue())
      case JBool(x) => new java.lang.Boolean(x)
      case JObject(x) =>
        val row = new TableRow()
        for ((k, v) <- x) {
          row.set(k, convert(v))
        }
        row
      case JArray(x) => new java.util.ArrayList[AnyRef] (x.map(JsonToTablerow.convert).asJava)
      case JSet(x) => fail() // not supported
    }

  }
  // scalastyle:on cyclomatic.complexity
}
