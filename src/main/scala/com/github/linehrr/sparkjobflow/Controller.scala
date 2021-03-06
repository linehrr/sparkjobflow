package com.github.linehrr.sparkjobflow

import com.github.linehrr.sparkjobflow.annotation.moduleDeprecated
import com.github.linehrr.sparkjobflow.stats.{IStatestore, SimpleStatestore}

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}

class Controller extends SimpleStatestore {
  self: IStatestore =>
  private[this] val logger = org.log4s.getLogger

  private val modules: mutable.Set[IModule] = mutable.HashSet()

  private def runProcessHandler(in: Seq[Any], module: IModule): Any = {
    try {
      self.markRunning(module.moduleName)
      val res = module.process(in)
      self.markFinish(module.moduleName)

      res
    }catch{
      case e: Throwable =>
        self.markFailed(module.moduleName, e)
        module.on_failure(e, in)
        module.failfast()
    }
  }

  private final def composeTopology(): Unit = {
    val futures: mutable.Map[String, Future[_]] = mutable.HashMap()
    val promises: mutable.Map[String, Promise[Any]] = mutable.HashMap()
    val complete: mutable.Map[String, mutable.HashSet[String]] = mutable.HashMap()
    val completedResults: mutable.Map[String, Any] = mutable.HashMap()

    def checkDependencies(module: IModule, dependedModule: String): Boolean = {
      complete.synchronized{
        if(!complete.contains(module.moduleName)){
          val completedSet = mutable.HashSet[String]()
          completedSet.add(dependedModule)
          complete.put(module.moduleName, completedSet)
        }else{
          val completedSet = complete(module.moduleName)
          completedSet.add(dependedModule)
        }

        module.depend.get.foreach(
          dependedModule => {
            if(!complete(module.moduleName).contains(dependedModule)) return false
          }
        )

        true
      }
    }

    modules.par.foreach(
      module => {
        val promise: Promise[Any] = Promise()
        promises.put(module.moduleName, promise)

        if (module.depend.isEmpty) {
          futures.put(
            module.moduleName,
            Future {
              promise.success(
                runProcessHandler(null, module)
              )
            }
          )
        } else {
          module.depend.get.foreach(
            dependedModule => {
              while (!promises.contains(dependedModule)) {
                logger.info(s"Waiting for dependency from ${module.moduleName} to ${module.depend.get}")
                Thread.sleep(100L)
              }
              val dependency = promises(dependedModule)

              dependency.future.onSuccess {
                case result =>
                  completedResults.put(dependedModule, result)
                  if(checkDependencies(module, dependedModule)) {

                    val input = module.depend.get.map(completedResults(_))

                    futures.put(
                      module.moduleName,
                      Future {
                        promise.success(
                          runProcessHandler(input, module)
                        )
                      }
                    )
                  }
              }
            }
          )
        }
      }
    )

    modules.foreach(
      m => {
        logger.info(s"waiting for future module ${m.moduleName}")

        while(!futures.contains(m.moduleName)){
          logger.info(s"waiting for future module ${m.moduleName} be to registered...")
          Thread.sleep(1000)
        }

        val f = futures(m.moduleName)
        Await.ready(f, Duration.Inf)
        logger.info(s"Completed module ${m.moduleName}")
      }
    )

    self.closeStatestore()
  }

  final def start(): Unit = {
    composeTopology()
  }

  final def register[T <: IModule](module: T): Controller = {
    if(module.getClass.isAnnotationPresent(classOf[moduleDeprecated])){
      val annotation = module.getClass.getAnnotation(classOf[moduleDeprecated])
      logger.info(s"Skip loading module ${module.moduleName} due to ${annotation.reason()}")
    }else{
      modules.add(module)
    }

    this
  }
}
