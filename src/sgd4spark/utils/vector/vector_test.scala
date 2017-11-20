package sgd4spark.utils.vector
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap
import sgd4spark.utils.Timer
import sgd4spark.utils.utils.plog

object vector_test {

  def pr(vec: Long2DoubleOpenHashMap): Unit = {
    val d = vec.keySet().toLongArray()
    val s = d.takeRight(10).map(k => s"$k: ${vec.get(k)}").mkString(", ")
    println(s"len=${d.length}: [$s]")
  }
  def test_dotself(u: LDVector): Unit = {
    plog("start test dot...")
    val t = new Timer
    for (i <- 1 to 100000) {
      val x = u.dotSelf()
    }
    plog(t.timer())
    pr(u.data)
  }
  def test_dot(u: LDVector, v: LDVector): Unit = {
    plog("start test dot...")
    val t = new Timer
    for (i <- 1 to 100000) {
      u.dot(v)
    }
    plog(t.timer())
    pr(u.data)
  }
  def test_add(u: LDVector, v: LDVector): Unit = {
    plog("start test dot...")
    val t = new Timer
    for (i <- 1 to 100000) {
      u.add(v)
    }
    plog(t.timer())
    pr(u.data)
  }
  def test_add2(u: LDVector, v: LDVector): Unit = {
    plog("start test dot...")
    val t = new Timer
    for (i <- 1 to 100000) {
      u.add(v, 0.1)
    }
    plog(t.timer())
    pr(u.data)
  }
  def test_times(u: LDVector): Unit = {
    plog("start test times...")
    val t = new Timer
    for (i <- 1 to 200000) {
      u.timesBy(1.0001)
    }
    plog(t.timer())
    pr(u.data)
  }
  def test_L1(u: LDVector): Unit = {
    plog("start test L1...")
    pr(u.data)
    val t = new Timer
    for (i <- 1 to 200000) {
      u.update_L1(0.1)
    }
    plog(t.timer())
    pr(u.data)
  }
  def test_performence(): Unit ={
    val u = new LDVector(20)
    val v = new LDVector(20)
    for (i <- 1 to 10000) u.set(i, i)
    // for (i <- 5000 to 15000) v.set(i, i/10)

    // test_dotself(u)
    // test_dot(u, v) // 0.23ms
    // test_add(u, v) // 0.18ms
    // test_add2(u, v) // 0.20ms
    // test_addtimes(u, v)
    // test_times(u)
    test_L1(u)
  }

  def prin(d: Long2DoubleOpenHashMap): Unit = {
    println(s"len: ${d.size()}")
    val iter = d.long2DoubleEntrySet().fastIterator()
    while (iter.hasNext) {
      val n = iter.next()
      println(s"${n.getLongKey}: ${n.getDoubleValue}")
    }
  }
  def test_LDHashMap_iterator(): Unit = {
    val data = new Long2DoubleOpenHashMap(10)
    for (i <- 1 to 3) data.put(i, i * 2)
    println(data.toString)
    println("do...")

    val iter = data.long2DoubleEntrySet().fastIterator()

    // println(s"len=${data.size()}: ${if (iter.hasNext) 1 else 0}")
    var n = iter.next()
    println(s"a ${n.getLongKey}: ${n.getDoubleValue}")

    data.remove(n.getLongKey)
    // println(s"len=${data.size()}: ${if (iter.hasNext) 1 else 0}")
    n = iter.next()
    println(s"b ${n.getLongKey}: ${n.getDoubleValue}")

    data.remove(n.getLongKey)
    // println(s"len=${data.size()}: ${if (iter.hasNext) 1 else 0}")
    n = iter.next()
    println(s"c ${n.getLongKey}: ${n.getDoubleValue}")

    println("after:")
    println(data.toString)
  }
  def main(args: Array[String]): Unit = {

    // test_performence()
    // System.exit(0)

    // test_LDHashMap_iterator()
    // System.exit(0)

    val u = new LDVector(20)
    val v = new LDVector(20)
    val w = new LDVector(20)
    u.set(1, 1.1)
    u.set(2, 2.2)
    u.set(3, 3.3)

    println("origin:")
    println(u.toString)

    var k = 2
    println(s"$k: ${u.get(k)}")
    k = 4
    println(s"$k: ${u.get(k)}")
    System.exit(0)
    return

  }
}
