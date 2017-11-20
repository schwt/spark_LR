package sgd4spark.work

import sgd4spark.utils.vector.LDVector

class oneInstance(dim: Int) extends Serializable {

  val x = new LDVector(dim)
  var y: Short = 1                // +-1

  def this(_dim: Int, s: String) {
    this(_dim)
    this.parse(s)
  }

  def get_index(): Array[Long] = x.get_index()

  def parse(s: String): Unit = {
    try {
      val arr = s.split(" ")
      y = if (arr(0).toFloat.toInt < 1) -1 else 1
      arr.takeRight(arr.length -1)
        .foreach{case s: String =>
          val f: Array[String] = s.split(":")
          x.set(f(0).toLong, f(1).toDouble)
        }
    } catch {
      case _: NumberFormatException => {}
    }
  }

}
