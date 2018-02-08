package com.github.linehrr.sparkjobflow.stats

import scala.collection.mutable

trait SimpleStatestore extends IStatestore {

  private final val simpleStats: mutable.Map[String, ModuleStatus] = mutable.HashMap[String, ModuleStatus]()

  override def markRunning(moduleName: String): Unit = {
    if(!simpleStats.contains(moduleName)) simpleStats(moduleName) = ModuleStatus()

    simpleStats(moduleName).startTime = System.currentTimeMillis()
  }

  override def markFinish(moduleName: String): Unit = {
    simpleStats(moduleName).finishTime = System.currentTimeMillis()
    simpleStats(moduleName).succeed = true
  }

  override def markFailed(moduleName: String, e: Throwable): Unit = {
    simpleStats(moduleName).finishTime = System.currentTimeMillis()
    simpleStats(moduleName).succeed = false
  }

  override def closeStatestore(): Unit = {
    simpleStats.foreach{
      case (moduleName, stats) =>
        println(s"Module: $moduleName")
        println(s"Starttime: ${stats.startTime}")
        println(s"Finishtime: ${stats.finishTime}")
        println(s"Duration: ${stats.finishTime - stats.startTime}")
        println("**********")
    }
  }

  case class ModuleStatus(
                         var startTime: Long = 0L,
                         var finishTime: Long = 0L,
                         var succeed: Boolean = false
                         )
}
