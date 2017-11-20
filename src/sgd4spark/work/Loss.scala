package sgd4spark.work
import scala.math._

object Loss {
  // L = log(1 + exp(-ya)), a=<x,w>
  def loss(a: Double, y: Double): Double = {
    val z = a * y
    if (z > 18.0)
      return exp(-z)
    if (z < -18.0)
      return -z
    log(1 + exp(-z))
  }

  // dloss = dL/da, a=<x,w>,  dL/dw_i = x_i dloss
  def dloss(a: Double, y: Double): Double = {
    val z = a * y
    if (z > 18.0)
      return -y * exp(-z)
    if (z < -18.0)
      return -y
    -y / (1 + exp(z))
  }

}
