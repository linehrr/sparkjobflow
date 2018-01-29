package com.github.linehrr.sparkjobflow.test

import com.github.linehrr.sparkjobflow.annotation.failFast
import com.github.linehrr.sparkjobflow.{Controller, IModule}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SparkTests extends App {
  val controller = new Controller
  controller
      .register(SparkModule1)
      .register(SparkModule2)
      .register(SparkModule3)

  controller.start()
}

object SparkModule1 extends IModule {
  override def moduleName = "M1"

  override def depend = None

  override def process(in: Seq[Any]) = {
    val ss: SparkSession =
      SparkSession.builder()
        .master("local[4]")
        .appName("Logic Test")
        .getOrCreate()
    val sc: SparkContext = ss.sparkContext

    val inputData = sc.makeRDD(
      List[String](
        "M1",
        "M2",
        "M3",
        "M4"
      )
    )

    inputData
  }
}

@failFast(exitCode = 9)
object SparkModule2 extends IModule {
  override def moduleName = "M2"

  override def depend = Option(Seq("M1"))

  override def process(in: Seq[Any]) = {
    in.head.asInstanceOf[RDD[String]].foreach(println)
    in.head.asInstanceOf[RDD[String]].map( r => r + "M2" )
    throw new Exception
  }
}

object SparkModule3 extends IModule {
  override def moduleName = "M3"

  override def depend = Option(Seq("M2", "M1"))

  override def process(in: Seq[Any]) = {
    in.head.asInstanceOf[RDD[String]].foreach(println)
    in(1).asInstanceOf[RDD[String]].map( _ + "M3").collect().foreach(println)
  }
}
