package com.github.linehrr.sparkjobflow.stats

trait IStatestore {
  def markRunning(moduleName: String): Unit

  def markFinish(moduleName: String): Unit

  def markFailed(moduleName: String, e: Throwable): Unit

  def closeStatestore(): Unit
}
