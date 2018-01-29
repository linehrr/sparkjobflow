package com.github.linehrr.sparkjobflow

import com.github.linehrr.sparkjobflow.annotation.moduleDeprecated

import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global

class Controller {
  private val modules: mutable.Set[IModule] = mutable.HashSet()

  private def runProcessHandler(in: Any, module: IModule): Any = {
    try {
      module.process(in)
    }catch{
      case e: Exception =>
        module.on_failure(e, in)
    }
  }

  private final def composeTopology(): Unit = {
    val futures: mutable.Set[Future[_]] = mutable.HashSet()
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
          futures.add(
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
                println(s"Waiting for dependency from ${module.moduleName} to ${module.depend.get}")
                Thread.sleep(100L)
              }
              val dependency = promises(dependedModule)

              dependency.future.onSuccess {
                case result =>
                  completedResults.put(dependedModule, result)
                  if(checkDependencies(module, dependedModule)) {

                    val input = module.depend.get.map(completedResults(_))

                    futures.add(
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

    futures.foreach(
      f => Await.ready(f, Duration.Inf)
    )
  }

  final def start(): Unit = {
    composeTopology()
  }

  final def register[T <: IModule](module: T): Controller = {
    if(module.getClass.isAnnotationPresent(classOf[moduleDeprecated])){
      val annotation = module.getClass.getAnnotation(classOf[moduleDeprecated])
      println(s"Skip loading module ${module.moduleName} due to ${annotation.reason()}")
    }else{
      modules.add(module)
    }

    this
  }
}
