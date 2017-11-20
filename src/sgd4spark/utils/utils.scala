package sgd4spark.utils

import java.text.SimpleDateFormat
import java.util.Date

object utils {

  def plog(s: String): Unit = {
    val form = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dt = form.format(new Date())
    println(s"[${dt}] $s")
  }

}
