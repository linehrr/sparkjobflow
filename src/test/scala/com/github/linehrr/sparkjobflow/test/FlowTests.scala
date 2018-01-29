package com.github.linehrr.sparkjobflow.test

import com.github.linehrr.sparkjobflow.{Controller, IModule}

object FlowTests extends App {

  val controller = new Controller

  controller
    .register(Module1)
    .register(Module2)
    .register(Module3)

  controller.start()

}

object Module1 extends IModule {
  override def moduleName = "M1"

  override def depend = None

  override def process(in: Any) = {
    println("I am M1")
    "M1 returned"
  }
}

object Module2 extends IModule {
  override def moduleName = "M2"

  override def depend = Option(Seq("M1"))

  override def process(in: Any) = {
    val casted = in.asInstanceOf[Seq[Any]]

    casted.head.asInstanceOf[String] + "---M2 returned"
  }
}

object Module3 extends IModule {
  override def moduleName = "M3"

  override def depend = Option(Seq("M1", "M2"))

  override def process(in: Any) = {
    val casted = in.asInstanceOf[Seq[Any]]

    println(casted(1).asInstanceOf[String])
  }
}