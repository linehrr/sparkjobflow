package com.github.linehrr.sparkjobflow

import com.github.linehrr.sparkjobflow.annotation.failFast

trait IModule {
  private[this] val logger = org.log4s.getLogger

  def moduleName: String

  def depend: Option[Seq[String]]

  def process(in: Seq[Any]): Any

  def on_failure(e: Throwable, in: Seq[Any]): Any = {
    // default failure handling
    logger.error(e)(s"Failed module $moduleName")

    null
  }

  final def failfast(): Unit = {
    if(getClass.isAnnotationPresent(classOf[failFast])){
      sys.exit(getClass.getAnnotation(classOf[failFast]).exitCode())
    }
  }
}