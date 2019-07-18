package com.arquivei.core.configs

import com.arquivei.core.RunEnvironment
import com.arquivei.core.exceptions.ArquiveiException
import org.json4s._
import org.scalatest.FlatSpec

class OptionsTest extends FlatSpec {

  "Options" should "set values correctly" in new RunEnvironment {
    Options.setValue(values)
    Options()().get.diff(values) match {
      case Diff(JNothing, JNothing, JNothing) =>
      case _ => fail
    }
  }
  it should "return user options" in new RunEnvironment {
    assertResult("test")(options.getOptionString("strOption"))
    assertResult(2)(options.getOptionInt("intOption"))
    assertResult(-4.5)(options.getOptionFloat("floatOption"))
    assertResult(true)(options.getOptionBoolean("boolOption"))
  }

  it should "return gcp project" in new RunEnvironment {
    assertResult("project")(options.gcpProject)
  }

  it should "throw error if key is missing or wrong type" in new RunEnvironment {
    assertThrows[ArquiveiException](options.getOptionString("missingOption"))
    assertThrows[ArquiveiException](options.getOptionInt("boolOption"))
  }
}
