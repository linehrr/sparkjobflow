package com.github.linehrr.sparkjobflow.test

import com.github.linehrr.sparkjobflow.annotation.{failFast, moduleDeprecated}
import com.github.linehrr.sparkjobflow.{Controller, IModule}
import org.apache.log4j.BasicConfigurator
import org.scalatest.FunSuite

class FlowTests extends FunSuite {

  BasicConfigurator.configure()

  val controller = new Controller

  controller
    .register(Module1)
    .register(Module2)
    .register(new Module3)
    .register(new Module4)
    .register(new Module5)

  test("Main test") {
    controller.start()
  }
}

@failFast
object Module1 extends IModule {
  override def moduleName = "M1"

  override def depend = None

  override def process(in: Seq[Any]) = {
    "M1"
  }
}

@failFast
object Module2 extends IModule {
  override def moduleName = "M2"

  override def depend = Option(Seq("M1"))

  override def process(in: Seq[Any]) = {
    in.head.asInstanceOf[String] + "M2"
  }
}

@failFast
class Module3 extends IModule {
  override def moduleName = "M3"

  override def depend = Option(Seq("M1", "M2"))

  override def process(in: Seq[Any]) = {
    assert(in.head.asInstanceOf[String] == "M1")
    assert(in(1).asInstanceOf[String] == "M1M2")
    123
  }
}

@failFast
class Module4 extends IModule {
  override def moduleName = "M4"

  override def depend = Option(Seq("M2", "M3"))

  override def process(in: Seq[Any]) = {
    assert(in(1).asInstanceOf[Int] == 123)
  }
}

@moduleDeprecated(reason = "test deprecation")
@failFast
class Module5 extends IModule {
  override def moduleName = "M5"

  override def depend = None

  override def process(in: Seq[Any]) = {
      assert(false)
  }
}