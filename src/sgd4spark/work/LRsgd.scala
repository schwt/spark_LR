package sgd4spark.work

import java.io.FileWriter
import scala.util.Random
import scala.math.{abs, exp}
import org.apache.spark.SparkContext
import sgd4spark.utils.vector.LDVector
import sgd4spark.utils.utils.plog

class LRsgd(_dim: Long, _lambda: Double, _eta0: Double) extends Serializable {

  var dim: Long      = _dim
  val lambda: Double = _lambda
  var eta0: Double   = _eta0
  var t                = 1
  var wBias            = 0.0D
  var w = new LDVector(dim)

  override def clone: LRsgd = {
    val result = new LRsgd(dim, lambda, eta0)
    result.wBias = this.wBias
    result.w = this.w.clone()
    result.t = this.t
    result
  }
  def plus(other: LRsgd): LRsgd = {
    w.add(other.w)
    wBias = wBias + other.wBias
    t = t + other.t
    this
  }
  def mean(other: LRsgd): LRsgd = {
    w.add(other.w)
    w.timesBy(0.5)
    wBias = (wBias + other.wBias) / 2
    t = (t + other.t) / 2
    this
  }
  def wnorm(): Double = {
    w.dotSelf() + wBias * wBias
  }
  def trainOne(point: oneInstance, eta: Double): Unit = {
    val s = w.dot(point.x) + wBias
    val d = Loss.dloss(s, point.y)
    w.timesBy(1.0 - lambda * eta)
    if (d != 0.0)
      w.add(point.x, - eta * d)
    val etab = 0.01 * eta
    wBias *= (1 - etab * lambda)
    wBias -= etab * d
  }
  def train(data: Array[oneInstance], shuffle: Boolean): Unit = {
    val num = data.length
    val indexes = if (!shuffle) {
      0.until(num).toList
    } else {
      val random = new Random(System.currentTimeMillis())
      random.shuffle(0.until(num).toList)
    }
    for (i <- indexes) {
      val eta = eta0 / (1 + lambda * eta0 * t)
      trainOne(data(i), eta)
      t += 1
    }
  }

  def predict_z(instance: oneInstance): Double = {w.dot(instance.x) + wBias }
  def evaluate(samples: Array[oneInstance]): Unit = {
    val yz = samples
      .map(d => (d.y, predict_z(d)))
      .sortWith((a,b) => a._2 > b._2)
    var (loss, nerr) = (0.0, 0.0)
    var (m, n, s) = (0, 0, 0.0)
    yz.foreach{case (y: Short, z: Double) => {
      loss += Loss.loss(z, y)
      nerr += (if (z * y < 0) 1.0 else 0.0)
      if (y > 0) {n += 1}
      else { m += 1; s += n}
    }}
    loss /= samples.length
    nerr /= samples.length
    val loss_w = 0.5 * lambda * wnorm()
    val auc = s / (m * n)
    plog(f"\tLoss = ${loss  + loss_w}%.5f ($loss%.6f + $loss_w%.6f)")
    plog(f"\tERT  = ${100.0 * nerr}%.4f%%")
    plog(f"\tAUC  = $auc%.5f")
  }

  def evaluateEta(data: Array[oneInstance], eta: Double)
  : Double = {
    val clones = this.clone
    for (d <- data)
      clones.trainOne(d, eta)
    var loss = 0.0
    for (d <- data) {
      loss += Loss.loss(clones.predict_z(d), d.y)
    }
    loss /= data.length
    val cost = loss + 0.5 * lambda * clones.wnorm()
    cost
  }
  def determineEta0(data: Array[oneInstance]): Unit = {
    val factor = 2.0
    var loEta = 1.0
    var hiEta = loEta * factor
    var loCost = evaluateEta(data, loEta)
    var hiCost = evaluateEta(data, hiEta)
    if (loCost < hiCost) {
      while (loCost < hiCost) {
        hiEta = loEta
        hiCost = loCost
        loEta = hiEta / factor
        loCost = evaluateEta(data, loEta)
        plog(s"eval eta0: ($loCost, $hiCost)")
      }
    } else if (hiCost < loCost) {
      while (hiCost < loCost) {
        loEta = hiEta
        loCost = hiCost
        hiEta = loEta * factor
        hiCost = evaluateEta(data, hiEta)
        plog(s"eval eta0: ($loCost, $hiCost)")
      }
    }
    eta0 = loEta
    plog(s"using eta0 = $eta0")
  }

  def save_text(dst: String): Unit = {
    val file = new FileWriter(dst)
    file.write(s"${wBias}\tBIAS")
    val arr_w = w.to_array().sortWith((a,b) => a._2 > b._2)
    arr_w.foreach(x => file.write(s"${x._1}\t${x._2}"))
    file.close()
  }
  def save_hdfs(sc: SparkContext, dst: String): Unit = {
    plog("saving...")
    val arr_w = w.to_array().sortWith((a,b) => abs(a._2) > abs(b._2))
    val arr_para = (Array((-1L, wBias)) ++ arr_w).map(x => s"${x._1}\t${x._2}")
    val rdd = sc.parallelize(arr_para, 1)
    rdd.saveAsTextFile(dst)
    plog("saved.")
  }
}
