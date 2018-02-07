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

## Interfaces
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

## Parallelism 
This flow controller will fully leverage the possibility of concurrency wherever it's possble.  
As long as it finds out that 2 modules are not depended on each other, they will be launched in parallel once dependencies are satisfied.  
Note: this is also 'spark-safe' and does not violate the lazy evaluation of the spark engine. If a module is only doing transformation,
this module will immediately return and thus letting downstream modules to take advantage of Catalyst engine([Spark Catalyst engine](https://community.hortonworks.com/articles/72502/what-is-tungsten-for-apache-spark.html)).  

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
