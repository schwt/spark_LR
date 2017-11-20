package sgd4spark.utils
import java.util.Date
import sgd4spark.utils.utils.plog

import scala.util.hashing.MurmurHash3.arrayHash

class Timer {
  var _kilo_second = current_time()

  def tagger(): Unit = {
    _kilo_second = current_time()
  }
  def timer(): String = {
    val diff = (current_time() - _kilo_second) / 1000.0
    tagger()
    val second = (diff % 60).toInt
    val minute = ((diff / 60) % 60).toInt
    val hour   = ((diff / 3600) % 60).toInt
    f"$hour%02d:$minute%02d:$second%02d"
  }
  def current_time(): Long = (new Date()).getTime
}


// for test
object Timer {
  def main(args: Array[String]): Unit = {
    plog("start...")
    val o = new Timer()
    Thread.sleep(1000*2)
    plog(o.timer)

    val arr = 0.to(1000).toArray
    val hashing = arrayHash(arr)
    val parts = 200
    plog(s"hashing: $hashing")
    plog(s"%: ${hashing % parts}")

  }
}
