package sgd4spark.tests

import org.apache.spark.SparkContext
import sgd4spark.utils.utils.plog

object test_spark {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext()

    plog("start...")
    val src = args(1)
    val cnt = src.split(",")
      .map(f => sc.textFile(f.trim))
      .map(_.count())
      .reduce(_ + _)
    plog(s"cnt: $cnt")
    sc.stop()
  }

}
