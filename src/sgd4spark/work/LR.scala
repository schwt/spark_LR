package sgd4spark.work

import java.io.FileWriter
import scala.util.Random
import scala.math.{abs, exp}
import org.apache.spark.SparkContext
import sgd4spark.utils.vector.LDVector
import sgd4spark.utils.utils.plog

class LR(_dim: Long, _lambda: Double, _eta0: Double, _regular_type: Int) extends Serializable {

  val dim: Long      = _dim
  val lambda: Double = _lambda
  var eta0: Double   = _eta0
  val regular: Int   = _regular_type     // 1: L1, 2: L2

  var t        = 1
  var wBias    = 0.0D
  var wDivisor = 1.0D
  var w = new LDVector(dim)

  def eta_t(): Double = eta0 / (1 + lambda * eta0 * t)
  def size(): Long = w.size
  def clear(): Unit = w.clear()

  override def clone: LR = {
    val result = new LR(dim, lambda, eta0, regular)
    result.w = this.w.clone()
    result.wBias = this.wBias
    result.wDivisor = this.wDivisor
    result.t = this.t
    result
  }
  def plus(other: LR): LR = {
    w.add(other.w)
    wBias = wBias + other.wBias
    t = t + other.t
    this
  }
  def mean(other: LR): LR = {
    w.add(other.w)
    w.timesBy(0.5)
    wBias = (wBias + other.wBias) / 2
    t = (t + other.t) / 2
    this
  }
  def div(x: Double): LR = {
    w.timesBy(1 / x)
    wBias /= x
    t = math.ceil(t / x).toInt
    this
  }
  def renorm(): Unit = {
    if (wDivisor != 1.0 && wDivisor != 0.0) {
      w.timesBy(1.0 / wDivisor)
      wDivisor = 1.0
    }
  }
  def wnorm(): Double = {
    var norm = w.dotSelf() / wDivisor / wDivisor
    norm += wBias * wBias
    norm
  }
  def _trainOne_L2(point: oneInstance, eta: Double): Unit = {
    val s = w.dot(point.x) / wDivisor + wBias
    wDivisor /= (1 - eta * lambda)
    if (wDivisor > 1e5)
      renorm()
    val d = Loss.dloss(s, point.y)
    if (d != 0.0)
      w.add(point.x, - eta * d * wDivisor)
    val etab = 0.01 * eta
    wBias *= (1 - etab * lambda)
    wBias -= etab * d
  }
  def _trainOne_L1(point: oneInstance, eta: Double): Unit = {
    val s = w.dot(point.x) + wBias
    val d = Loss.dloss(s, point.y)
    if (d != 0.0)
      w.add(point.x, -eta * d)
    w.update_L1(eta * lambda)
    val etab = 0.01 * eta
    wBias -= etab * d
    val delta = etab * lambda
    if (abs(wBias) < delta) wBias = 0
    else wBias += {if (wBias>0) -delta else delta}
  }
  def trainOne(point: oneInstance, eta: Double): Unit = {
    if (regular == 1) _trainOne_L1(point, eta)
    else if (regular == 2) _trainOne_L2(point, eta)
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
      trainOne(data(i), eta_t())
      t += 1
    }
    // if (regular == 1) w.prune_L1(lambda * eta_t())
    if (regular == 2) renorm()
  }

  def predict_z(instance: oneInstance): Double = {
    w.dot(instance.x) / wDivisor + wBias
  }

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

  def costEta(data: Array[oneInstance], eta: Double)
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
    var loCost = costEta(data, loEta)
    var hiCost = costEta(data, hiEta)
    if (loCost < hiCost) {
      while (loCost < hiCost) {
        hiEta = loEta
        hiCost = loCost
        loEta = hiEta / factor
        loCost = costEta(data, loEta)
        plog(s"find Eta0: $loEta ($loCost, $hiCost)")
      }
    } else if (hiCost < loCost) {
      while (hiCost < loCost) {
        loEta = hiEta
        loCost = hiCost
        hiEta = loEta * factor
        hiCost = costEta(data, hiEta)
        plog(s"find Eta0: $loEta ($loCost, $hiCost)")
      }
    }
    eta0 = loEta
    plog(s"using eta0 = $eta0")
  }

  def _save_native(dst: String): Unit = {
    val file = new FileWriter(dst)
    file.write(s"$wBias\tBIAS")
    val arr_w = w.to_array().sortWith((a,b) => a._2 > b._2)
    arr_w.foreach(x => file.write(s"${x._1}\t${x._2}"))
    file.close()
  }
  def _save_hdfs(sc: SparkContext, dst: String): Unit = {
    val arr_w = w.to_array().sortWith((a,b) => abs(a._2) > abs(b._2))
    val arr_para = (Array((-1L, wBias)) ++ arr_w).map(x => s"${x._1}\t${x._2}")
    val rdd = sc.parallelize(arr_para, 1)
    rdd.saveAsTextFile(dst)
    plog("saved.")
  }
  def save(sc: SparkContext, dst: String): Unit = {
    plog("saving...")
    if (regular == 2) renorm()
    if (regular == 1) w.prune_L1(lambda * eta_t())
    plog(s"Final:")
    plog(s"@       t = $t")
    plog(s"@     eta = ${eta_t()}")
    plog(s"@   beta0 = $wBias")
    plog(s"@   model = ${w.size}")
    if (regular == 1)
      plog(s"@  thresh = ${lambda * eta_t()}")
    if (dst.take(4) == "hdfs") _save_hdfs(sc, dst)
    if (dst.take(4) == "file") _save_native(dst)
  }
}
