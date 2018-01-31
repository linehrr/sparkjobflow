package com.github.linehrr.sparkjobflow.test

import com.github.linehrr.sparkjobflow.annotation.failFast
import com.github.linehrr.sparkjobflow.{Controller, IModule}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class SparkTests extends FunSuite {
  val controller = new Controller
  controller
      .register(SparkModule1)
      .register(SparkModule2)
      .register(SparkModule3)

  test("Spark Logic Main test"){
    controller.start()
  }
}

@failFast
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
        "M1"
      )
    )

    inputData
  }
}

@failFast
object SparkModule2 extends IModule {
  override def moduleName = "M2"

  override def depend = Option(Seq("M1"))

  override def process(in: Seq[Any]) = {
    assert(in.head.asInstanceOf[RDD[String]]
      .collect()
      .head == "M1")

    in.head.asInstanceOf[RDD[String]].map( r => r + "M2" )
  }
}

@failFast
object SparkModule3 extends IModule {
  override def moduleName = "M3"

  override def depend = Option(Seq("M2", "M1"))

  override def process(in: Seq[Any]) = {
    assert(in.head.asInstanceOf[RDD[String]]
      .collect()
      .head == "M1M2")

    assert(in(1).asInstanceOf[RDD[String]]
      .map( _ + "M3")
      .collect()
      .head == "M1M3"
    )
  }
}
