package com.github.linehrr.sparkjobflow

trait IModule {
  def moduleName: String

  def depend: Option[Seq[String]]

  def process(in: Seq[Any]): Any

  def on_failure(e: Throwable, in: Any): Any = {
    // default failure handling
    e.printStackTrace()
    null
  }
}
