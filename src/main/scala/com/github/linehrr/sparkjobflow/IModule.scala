package com.github.linehrr.sparkjobflow

trait IModule {
  private[this] val logger = org.log4s.getLogger

  def moduleName: String

  def depend: Option[Seq[String]]

  def process(in: Seq[Any]): Any

  def on_failure(e: Throwable, in: Any): Any = {
    // default failure handling
    logger.error(e)("Failed module")
    null
  }
}
