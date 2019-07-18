package com.arquivei.core.formats

import com.google.api.services.bigquery.model.TableRow
import org.json4s.JsonAST._
import org.scalatest.FlatSpec

class JsonTest extends FlatSpec {
  "anyToJValue" should "convert Any to JValue correctly" in {

    val bigInt: BigInt = 100000000
    val char: Char = 'a'
    val scalaMap = Map("oi"->1241,"tchau"->"lalalala")
    val str = "oi"
    val short: Short = 1.toShort
    val int: Int = 1
    val byte: Byte = 1.toByte
    val list = List(1,2,3)
    val float: Float = (13.9989).toFloat
    val long: Long = 1
    val double: Double = 1.2
    val bigDecimal: BigDecimal = 1.2
    val boolean = true

    case class Person(name: String, age: Int)

    val joao = Person("Joao", 30)

    val tableRow = new TableRow()
      .set("a", new TableRow().set("b", "c"))
      .set("b", List[TableRow](new TableRow().set("a", 2), new TableRow().set("b",3)))

    val bigIntJExpected = JInt(100000000)
    val charJExpected = JInt(97)
    val scalaMapJExpected = JObject(List(("oi",JInt(1241)), ("tchau",JString("lalalala"))))
    val strJExpected = JString("oi")
    val shortJExpected = JInt(1)
    val intJExpected = JInt(1)
    val byteJExpected = JInt(1)
    val listJExpected = JArray(List(JInt(1), JInt(2), JInt(3)))
    val longJExpected = JInt(1)
    val floatJExpected = JDouble(float)
    val doubleJExpected = JDouble(1.2)
    val bigDecimalJExpected = JDouble(1.2)
    val booleanJExpected = JBool(true)
    val tableRowJExpected = JObject(List(("a",JObject(List(("b",JString("c"))))), ("b",JArray(List(JObject(List(("a",JInt(2)))), JObject(List(("b",JInt(3)))))))))

    assertResult(bigIntJExpected)(Json.anyToJValue(bigInt))
    assertResult(charJExpected)(Json.anyToJValue(char))
    assertResult(scalaMapJExpected)(Json.anyToJValue(scalaMap))
    assertResult(strJExpected)(Json.anyToJValue(str))
    assertResult(shortJExpected)(Json.anyToJValue(short))
    assertResult(intJExpected)(Json.anyToJValue(int))
    assertResult(byteJExpected)(Json.anyToJValue(byte))
    assertResult(listJExpected)(Json.anyToJValue(list))
    assertResult(longJExpected)(Json.anyToJValue(long))
    assertResult(floatJExpected)(Json.anyToJValue(float))
    assertResult(doubleJExpected)(Json.anyToJValue(double))
    assertResult(bigDecimalJExpected)(Json.anyToJValue(bigDecimal))
    assertResult(booleanJExpected)(Json.anyToJValue(boolean))
    assertResult(tableRowJExpected)(Json.anyToJValue(tableRow))
    assertThrows[RuntimeException](Json.anyToJValue(joao))
  }

  "jValueToString" should "convert a JValue to a String" in {
    assertResult("")(Json.jValueToString(JNothing))
    assertResult("")(Json.jValueToString(JNull))
    assertResult("blabla")(Json.jValueToString(JString("blabla")))
    assertResult("1.2")(Json.jValueToString(JDouble(1.2)))
    assertResult("1.2")(Json.jValueToString(JDecimal(1.2)))
    assertResult("122")(Json.jValueToString(JLong(122)))
    assertResult("122")(Json.jValueToString(JInt(122)))
    assertResult("true")(Json.jValueToString(JBool(true)))
    assertThrows[Exception](Json.jValueToString(JArray(List(JInt(1)))))
  }


}
