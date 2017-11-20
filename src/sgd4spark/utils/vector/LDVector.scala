package sgd4spark.utils.vector
import breeze.linalg.min
import breeze.numerics.abs
import scala.collection.mutable.ListBuffer
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap

/*
 * Long-Double vector
 * T: performence test with 10000-dimention
 */

class LDVector (_dim: Long) extends Serializable {

  val dim: Int  = _dim.toInt
  var data = new Long2DoubleOpenHashMap(dim)
  data.defaultReturnValue(0.0D)

  def size: Int = data.size()
  def get(key: Long): Double = data.get(key)
  def set(key: Long, value: Double): Unit = {data.put(key, value)}
  def remove(key: Long): Unit = {data.remove(key)}
  def get_index(): Array[Long] = {data.keySet().toLongArray}
  def to_array(): Array[(Long, Double)] = data.keySet().toLongArray().map(k => (k, data.get(k)))
  def clear(): Unit = {data.clear()}

  override def clone(): LDVector = {
    val vec = new LDVector(this.dim)
    vec.data = vec.data.clone()
    vec
  }

  // T=0.07ms
  def dotSelf(): Double = {
    val iterator = data.long2DoubleEntrySet().fastIterator()
    var sum = 0.0D
    while (iterator.hasNext) {
      val n = iterator.next()
      val value = n.getDoubleValue
      sum += value * value
    }
    sum
  }

  // T=0.09ms
  def timesBy(x: Double): Unit = {
    val iterator = data.long2DoubleEntrySet().fastIterator()
    while (iterator.hasNext) {
      val n = iterator.next()
      val value = n.getDoubleValue
      n.setValue(x*value)
    }
  }
  // T=0.23ms
  def dot(other: LDVector): Double = {
    var sum = 0.0
    val iterator = other.data.long2DoubleEntrySet().fastIterator()
    while (iterator.hasNext) {
      val n = iterator.next()
      sum += data.get(n.getLongKey) * n.getDoubleValue
    }
    sum
  }
  // T=0.18ms
  def add(other: LDVector): Unit = {
    val iterator = other.data.long2DoubleEntrySet().fastIterator()
    while (iterator.hasNext) {
      val n = iterator.next()
      val key = n.getLongKey
      val value = data.get(key)
      data.put(key, value + n.getDoubleValue) // ?
    }
  }
  // T=0.2ms
  def add(other: LDVector, coef: Double): Unit = {
    val iterator = other.data.long2DoubleEntrySet().fastIterator()
    while (iterator.hasNext) {
      val n = iterator.next()
      val key = n.getLongKey
      val value = data.get(key)
      data.put(key, value + n.getDoubleValue * coef)
    }
  }
  // T = 0.03~0.1s
  def update_L1(x: Double): Unit = {
    val iterator = data.long2DoubleEntrySet.fastIterator
    val zero_keys = new ListBuffer[Long]
    while (iterator.hasNext) {
      val n = iterator.next()
      val value = n.getDoubleValue
      if (abs(value) < x)
        zero_keys.append(n.getLongKey)
      else
        n.setValue(if (value>0) value-x else value+x)
    }
    zero_keys.foreach{x => data.remove(x)}
  }
  def prune_L1(threshold: Double): Unit = {
    val iterator = data.long2DoubleEntrySet.fastIterator
    val zero_keys = new ListBuffer[Long]
    while (iterator.hasNext) {
      val n = iterator.next()
      if (abs(n.getDoubleValue) < threshold)
        zero_keys.append(n.getLongKey)
    }
    zero_keys.foreach{x => data.remove(x)}
  }

  override def toString: String = {
    val bdr = new StringBuilder
    bdr.append(s"[${this.size}/${this.dim}](")
    val num = min(this.size, 10)
    val iterator = this.data.long2DoubleEntrySet.fastIterator

    for (j <- 1 to num)  {
      val entry = iterator.next
      bdr.append(s"${entry.getLongKey}:${entry.getDoubleValue}")
      if (j < num) bdr.append(", ")
    }
    bdr.append(")")
    bdr.toString
  }
}

