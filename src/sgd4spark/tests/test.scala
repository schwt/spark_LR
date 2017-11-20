package sgd4spark.tests

object test {

  def main(args: Array[String]): Unit = {
    var xx = 0.1
    try {
      xx = "123c".toDouble
    } catch {
      case e: NumberFormatException => println("error")
    }
    finally {}
    println(xx)
  }

}
