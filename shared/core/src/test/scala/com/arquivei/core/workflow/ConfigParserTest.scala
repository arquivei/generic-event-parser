package com.arquivei.core.workflow

import com.arquivei.core.RunEnvironment
import com.arquivei.core.configs.Options
import org.json4s._
import org.scalatest._
import org.scalatest.Matchers._

class ConfigParserTest extends FlatSpec {
  "ConfigParser" should "convert json to flags" in new RunEnvironment {
    val args = ConfigParser.getArgs(values)
    val expected = List(
      "--project=project",
      "--numWorkers=1",
      "--workerMachineType=n1-standard-2",
      """--labels={"costcenter":"bi-development","infratype":"pipeline"}"""
    )
    assertResult(expected)(args)
  }
  it should "read yaml files" in {
    ConfigParser("run", "shared/core/src/test/resources/env.yaml.dist")
    val config = Options().get
    assertResult(JString("dev"))(config \ "environment")
    assertResult(JString("WARN"))(config \ "runner" \ "workerLogLevelOverrides" \ "org.apache.kafka.clients.consumer.internals.Fetcher")
    assertResult(JInt(1))(config \ "runner" \ "numWorkers")
    assertResult(JArray(List(JString("topic1"),JString("topic2"))))(config \ "connections" \ "kafka" \ "topic")
  }
  "updateFlags" should "handle update mode" in {
    val args = ConfigParser("update", "shared/core/src/test/resources/env.yaml.dist")
    val expected = List(
      "--update",
      """--transformNameMapping={"map1":"map2"}"""
    )
    for(flag <- expected) {
      assert(args.contains(flag))
    }
  }
  it should "handle empty transformNameMapping"  in {
    val args = ConfigParser("update", "shared/core/src/test/resources/nomap.yaml.dist")
    val expected = List(
      "--update",
      """--transformNameMapping={}"""
    )
    for(flag <- expected) {
      assert(args.contains(flag))
    }
  }
  it should "return the correct flags" in {
    val result = ConfigParser("run", "shared/core/src/test/resources/env.yaml.dist", Array("--testFlag=test"))
    val expected = List(
      "--runner=dataflow",
      "--project=project",
      "--jobName=myproject",
      "--zone=us-east1-b",
      "--network=default",
      "--stagingLocation=stag",
      "--tempLocation=temp",
      "--numWorkers=1",
      "--maxNumWorkers=1",
      "--workerMachineType=n1-standard-2",
      "--diskSizeGb=null",
      "--autoscalingAlgorithm=THROUGHPUT_BASED",
      "--labels={\"costcenter\":\"bi-development\",\"infratype\":\"pipeline\"}",
      "--workerLogLevelOverrides={\"org.apache.kafka.clients.consumer.internals.Fetcher\":\"WARN\"}",
      "--testFlag=test",
    )
    expected should contain theSameElementsAs result
  }

  "configOverrides" should "return the correct override option" in {
    val extraArgs = Array(
      "--runner=direct", "--random=42", "invalid", "99999", "--override1=abc", "--override2=4", "--override3=4.5"
    )
    val result = ConfigParser.configOverrides(extraArgs, List("override1", "override2", "override3"))
    val expected = JObject(
      "options" -> JObject(
        "override1" -> JString("abc"),
        "override2" -> JInt(4),
        "override3" -> JDouble(4.5)
      )
    )

    assertResult(expected)(result)
  }
}
