package com.arquivei.core.io.db.bigquery

import com.arquivei.core.exceptions.ArquiveiException
import com.google.api.services.bigquery.model.TableRow
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.scalatest.{FlatSpec, PrivateMethodTester}

class JsonToTablerowTest extends FlatSpec with PrivateMethodTester {
  "apply" should "convert values correctly" in {
    val json =
      """
        |{
        | "nullable": null,
        | "nothing": [],
        | "nothing2": {},
        | "nothing3": {"testing":{}, "testingAgain":[{"a":1,"b":3,"c":[]}, {}], "batata": 1},
        | "nothing4": [[]],
        | "nothing5": [{}],
        | "double": 1204109401924.1244,
        | "boolean": true,
        | "str": "value",
        | "int": 1,
        | "obj": {
        |   "str": "value2"
        | },
        | "arr1": [1,2],
        | "arr2": [
        |   {"str": "value3"}
        | ]
        |}
      """.stripMargin

    val obj = parse(json)
    val tableRow = JsonToTablerow(obj)
    val long: java.lang.Long = new java.lang.Long(1020410240)
    val double: java.lang.Double = new java.lang.Double(1204109401924.1244)

    assertResult(double)(tableRow.get("double").asInstanceOf[java.lang.Double])
    assertResult("value")(tableRow.get("str").asInstanceOf[java.lang.String])
    assertResult(1)(tableRow.get("int"))
    assertResult("value2")(tableRow.get("obj").asInstanceOf[TableRow].get("str"))
    assertResult(2)(tableRow.get("arr1").asInstanceOf[java.util.List[AnyRef]].size())
    assertResult("value3")(tableRow.get("arr2").asInstanceOf[java.util.List[AnyRef]].get(0).asInstanceOf[TableRow].get("str"))
    assertResult(null)(tableRow.get("none"))
    assertResult(null)(tableRow.get("nullable"))
    assertResult(null)(tableRow.get("nothing"))
    assertResult(null)(tableRow.get("nothing2"))
    assertResult(null)(tableRow.get("nothing3").asInstanceOf[TableRow].get("testing"))
    assertResult(null)(tableRow.get("nothing3").asInstanceOf[TableRow].get("testingAgain").asInstanceOf[java.util.List[AnyRef]].get(0).asInstanceOf[TableRow].get("c"))
    assertResult(1)(tableRow.get("nothing3").asInstanceOf[TableRow].get("testingAgain").asInstanceOf[java.util.List[AnyRef]].size())
    assertResult(null)(tableRow.get("nothing4"))
    assertResult(null)(tableRow.get("nothing5"))
    assertResult(tableRow.hashCode())(tableRow.clone.hashCode())

    val remaining =
      JObject(
        List(
          ("name", JString("Marilyn")),
          ("age", JInt(33)),
          ("account_ammount", JLong(1020410240)),
          ("account_ammount2", JDecimal(1204109401924.1244))
        )
      )
    val tableRowRemaining = JsonToTablerow(remaining)

    assertResult(long)(tableRowRemaining.get("account_ammount").asInstanceOf[java.lang.Long])
    assertResult(double)(tableRowRemaining.get("account_ammount2").asInstanceOf[java.lang.Double])
  }

  it should "handle empty Json" in {
    val json = "{}"
    val obj = parse(json)
    val tableRow = JsonToTablerow(obj)
    assert(tableRow != null, "tableRow != null")
    assert(tableRow.entrySet().isEmpty, "tableRow should be empty")
  }

  it should "accept only JObject" in {
    assertThrows[Exception](JsonToTablerow(JInt(0)))
    assertThrows[Exception](JsonToTablerow(JString("test")))
    assertThrows[Exception](JsonToTablerow(JArray(List(JString("test")))))
    assertThrows[Exception](JsonToTablerow(JNull))
    assertThrows[Exception](JsonToTablerow(JNothing))
  }
  val convert = PrivateMethod[AnyRef]('convert)
  "convert" should "fail on unsuported types" in {
    assertThrows[ArquiveiException](JsonToTablerow invokePrivate convert(JNull))
    assertThrows[ArquiveiException](JsonToTablerow invokePrivate convert(JNothing))
    assertThrows[ArquiveiException](JsonToTablerow invokePrivate convert(JSet(Set.empty[JValue])))
  }


}
