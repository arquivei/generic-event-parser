package com.arquivei.core.formats

import org.json4s.JsonDSL._
import org.json4s._

import scala.collection.JavaConverters._
import scala.language.implicitConversions

object Json {
  // scalastyle:off cyclomatic.complexity
  implicit def anyToJValue(value: Any): JValue = {
    value match {
      case x: String => x
      case x: Short => x
      case x: Byte => x
      case x: Char => x
      case x: Int => x
      case x: Long => x
      case x: BigInt => x
      case x: Double => x
      case x: Float => x
      case x: BigDecimal => x
      case x: Boolean => x
      case x: List[_] => x.map(Json.anyToJValue)
      case x: Map[_, _] => x.asInstanceOf[Map[String, Any]].mapValues(Json.anyToJValue)
      case x: java.util.Map[_, _] => Json.anyToJValue(x.asScala.toMap)
      case x: java.util.List[_] => Json.anyToJValue(x.asScala.toList)
      case null => JNull
      case _ => throw new RuntimeException("Unsupported data type " + Option(value).fold("null")(_.getClass.getName))
    }
  }
  // scalastyle:on cyclomatic.complexity
  def jValueToString(value: JValue): String = {
    value match {
      case JNothing => ""
      case JNull => ""
      case JString(x) => x
      case JDouble(x) => x.toString
      case JDecimal(x) => x.toString
      case JLong(x) => x.toString
      case JInt(x) =>x.toString
      case JBool(x) =>x.toString
      case _ => throw new Exception("Unsupported type: " + value.getClass.toString)
    }
  }
}
