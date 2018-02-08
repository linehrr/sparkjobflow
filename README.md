# sparkjobflow
This is a project to modulize spark jobs into flow fashion, it handles module dependencies as well and supports parallel job running. Could even be used for general purposes.

## Usage:
```scala
object Myclass extends App {
    val controller = new Controller
    controller
      .register(Module1)
      .register(new Module2)
    controller.start()
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
class Module2 extends IModule {
  override def moduleName = "M2"

  override def depend = Option(Seq("M1"))

  override def process(in: Seq[Any]) = {
    in.head.asInstanceOf[String] + "M2"
  }
}
```

## Module Interface
Each module has to extend from IModule trait. there are 3 required methods to implement and a few optional.  
```scala
trait IModule {
  // unique to each module
  def moduleName: String
  
  // seq of depended modules
  // order of the seq is important because it decides what order of the outputs will be passed in to this module
  // from depended modules
  def depend: Option[Seq[String]]

  // main process logic one needs to implement
  // Seq[Any] follows the order of the depend Seq, and will pass in output from upstream module as Any
  // therefore casting is required and will be loosely checked
  // possible InValidCast exception might be thrown if cast improperly.
  def process(in: Seq[Any]): Any
}
```

## Statestore Interface
Statestore is a mechanism to store each module's running/finish/fail states.  
Below is the interface definition:
```scala
  def markRunning(moduleName: String): Unit

  def markFinish(moduleName: String): Unit

  def markFailed(moduleName: String, e: Throwable): Unit

  def closeStatestore(): Unit
```
statestore has the same life-cycle as controller. they are instantiated and killed at the same time.  
`closeStatestore()` will be called right before controller ends its life cycle.  So all the data flushes and connection clean up should be done there.  
Controller will also use default SimpleStatestore embedded inside if no custom statestore is provided.  It will print out some simple stats about each module in the end:  
```text
Module: M1
Starttime: 1518122157017
Finishtime: 1518122159833
Duration: 2816
**********
Module: M3
Starttime: 1518122160367
Finishtime: 1518122160490
Duration: 123
**********
Module: M2
Starttime: 1518122159834
Finishtime: 1518122160367
Duration: 533
**********
```
otherwise, custom statestore could be injected by using cake-pattern:
```scala
trait MyStatestore {
   private final val db = connect2DB()

   def markRunning(moduleName: String): Unit = {
      db.markRunning(moduleName)
   }
   
   def markFinish(moduleName: String): Unit = {
      db.markFinish(moduleName)
   }
   
   def markFailed(moduleName: String, e: Throwable): Unit = {
      db.markFailed(moduleName, e.getMessage)
   }
   
   def closeStatestore(): Unit = {
      db.close()
   }
}

object myApp extends App {
   val controller = new Controller with MyStatestore
     ...
}

```

## On failure callbacks
One could override func in `IModule::on_failure(e: Throwable, in: Seq[Any])`  
`e: Throwable` is the throwables for the reason.  
`in: Seq[Any]` is the input of the failed module, so you can add some custom failover logic  
`super()` will give you nice logging implemented by default

## Dependency management
Dependency could only work in this project if the dependency graph is actually a [DAG](https://en.wikipedia.org/wiki/Directed_acyclic_graph). Cyclic graph will possibly cause deadloop and if not, unpredicted results.  
Interface `def depend: Option[Seq[String]]` is an Option and leaving is as None meaning it's the entry of the flow.  
and certainly one could create multiple entries and since they don't depend on each other, all entries will be executed in parallel.

## Parallelism 
This flow controller will fully leverage the possibility of concurrency wherever it's possble.  
As long as it finds out that 2 modules are not depended on each other, they will be launched in parallel once dependencies are satisfied.  
Note: this is also 'spark-safe' and does not violate the lazy evaluation of the spark engine. If a module is only doing transformation,
this module will immediately return and thus letting downstream modules to take advantage of Catalyst engine([Spark Catalyst Engine](https://community.hortonworks.com/articles/72502/what-is-tungsten-for-apache-spark.html)).  

example:
graph-A  
![DAG image](https://upload.wikimedia.org/wikipedia/commons/thumb/f/fe/Tred-G.svg/252px-Tred-G.svg.png)


moduleB and moduleC will be executed in parallel and moduleD will be a barrier. 


## Checkpointing
This is very essential to the performance of this flow controller. If we carefully design the checkpoints(caching), we could avoid many recomputations and IO overhead.  
See example below:
```scala
object myApp extends App {
  val controller = new Controller
  controller.register(Module1)
            .register(Module2)
            .register(Module3)
            
  controller.start()
}

Object Module1 extends IModule {
  override def moduleName = "M1"

  override def depend = None

  override def process(in: Seq[Any]) = {
    val rdd = <read from a file and generate rdd>
    rdd.map[String](<map func to perform some transforms>)
  }
}

Object Module2 extends IModule {
  override def moduleName = "M2"
  
  override def depend = Option(Seq("M1"))
  
  override def process(in: Seq[Any]) = {
    in.head.asInstanceOf[RDD[String]].map(<some other transformations>)
      .save(...)
  }
}

Object Module3 extends IModule {
  override def moduleName = "M3"
  
  override def depend = Option(Seq("M1"))
  
  override def process(in: Seq[Any]) = {
    in.head.asInstanceOf[RDD[String]].map(<some other transformations>)
      .save(...)
  }
}
```
the above example demostrated that both M2 and M3 depend on M1 and M1 is only doing some narrow transformation.  
therefore, according to spark's lazy eval, M1's transformation will be called and computed twice, thus defeats the purpose of this flow control.  
to amend this, we need to force M1 to eval and persist the result so that M2 and M3 can use the output of M1 without recompute everything.
```scala
Object Module1 extends IModule {
  override def moduleName = "M1"

  override def depend = None

  override def process(in: Seq[Any]) = {
    val rdd = <read from a file and generate rdd>
    val cachedRDD = rdd.map[String](<map func to perform some transforms>)
       .persist(<choose what StorageLevel fits>)
       
    cachedRDD.count() // this is forcing the eval of the trans, and count() is light weight so overhead is low
    
    cachedRDD
  }
}
```
Looking at graph-A again, DAG like that, if a,b,c,d are all transformations only e triggers action, we should then put checkpoint on a,c,d. in this case b is not needed since it's a one-input-one-output module.

## Annotations
available annotations:
```scala
// marking a module failFast with exitCode will force controller to call sys.exit when module fails
public @interface failFast {
    int exitCode() default 1;
}

// marking a module deprecated will skip the module loading of this module, thus won't be executed
// this could potentially break the DAG and leads to infinite waiting
// carefully analyze the dependency relations before deprecating a module
public @interface moduleDeprecated {
    String reason() default "Unknown";
}
```
## Logging
This project uses log4s logging facade for logging.  Since it's a wrapper for slf4j, therefore all popular solid logging impls are supported.

## Drawbacks
This project defines input and output of each module loosly as `Any.type`. this is flexible in a way but more dangerous when talking about RunTimeExceptions. Possible improvement would be making it strongly typed and let compiler to check type correctness for us. this will be future work.

