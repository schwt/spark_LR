package sgd4spark.work

import scala.math._
import scala.util.hashing.MurmurHash3.arrayHash
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import sgd4spark.utils.utils.plog
import sgd4spark.utils.Timer

object manage {

  var lambda     = 0.5
  // val lambda     = 1.0E-007D
  // val eta0       = 0.0078125D
  var eta0       = 0.005
  var max_dim    = 20
  var max_epoche = 10
  var scan_times = 10
  var parts      = 30
  val worker_num = 1
  var depth      = 5

  def main(args: Array[String]): Unit = {

    plog("start...")
    if (args.length < 7) {
      System.err.println("Error arguments!")
      System.exit(1)
    }
    val timer = new Timer
    val input_path  = args(0)
    val output_path = args(1)
    max_dim    = args(2).toInt
    max_epoche = args(3).toInt
    scan_times = args(4).toInt
    lambda     = args(5).toDouble
    eta0       = args(6).toDouble
    parts      = args(7).toInt
    depth = floor(log(parts) / log(2.0)).toInt

    plog(s"input data: $input_path")
    plog(s"max dim   = $max_dim")
    plog(s"expoches  = $max_epoche")
    plog(s"scan time = $scan_times")
    plog(s"lambda    = $lambda")
    plog(s"eta_0     = $eta0")
    plog(s"parts     = $parts")
    plog(s"red depth = $depth")

    val spark_conf = new SparkConf()
      .set("spark.driver.maxResultSize", "0")
      .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")

    spark_conf.getAll.foreach{ p => plog(s"\tSpark Conf: ${p._1} = ${p._2}")}

    val sc = new SparkContext(spark_conf)
    sc.hadoopConfiguration.setBoolean("mapred.output.compress", false)

    val data = load_samples(sc, input_path)
    plog(s"data partitions: ${data.getNumPartitions}")
    val train_set = arrange_sample(data).persist(StorageLevel.MEMORY_AND_DISK_SER)
    val test_set = data.take(100000)
    plog(s"train partitions: ${train_set.getNumPartitions}")

    val model = train(sc, train_set, test_set)
    model.save(sc, output_path)
/*
    val data = load_sample(sc, input_path).randomSplit(Array(0.7, 0.3))
    val train_set = data(0).collect()
    val test_set  = data(1).collect()
    plog(s"train size: ${train_set.length}")
    plog(s"test  size: ${test_set.length}")
    val timer = new Timer()
    val model = new LR(max_dim, lambda, eta0)
    // model.determineEta0(data)
    for (i <- 1 to scan_times) {
      plog(s"train loop [$i]...")
      model.train(train_set, true)
      model.evaluate(train_set)
      model.evaluate(test_set)
    }
    plog(timer.timer())
    model.save_hdfs(sc, output_path)
    */
    plog(s"total time: ${timer.timer()}")
    sc.stop()

  }
  def load_samples(sc: SparkContext, input_paths: String): RDD[oneInstance] = {
    plog("load data ...")
    val _max_dim = max_dim
    input_paths.split(",")
      .map(f => sc.textFile(f.trim))
      .reduce{(r1, r2) => r1.union(r2)}
      .map(s => {
        val data = new oneInstance(_max_dim, s)
        val valid: Short = if (data.x.size > 1) 1 else 0
        (valid, data)
      })
      .filter(x => x._1 == 1)
      .map(x => x._2)
  }
  def arrange_sample(rdd: RDD[oneInstance]): RDD[(Int, Array[oneInstance])] = {
    val _parts = parts
    rdd.groupBy(d => abs(arrayHash(d.get_index())) % _parts)   // RDD[(partid, iterb[oneInstance])]
      .map(x => (x._1, x._2.toArray))                          // RDD[(partid, Array[oneInstance])]
  }
  def train(sc: SparkContext, train_set: RDD[(Int, Array[oneInstance])], test_set: Array[oneInstance]): LR = {
    plog("start train ...")
    var last_model = new LR(max_dim, lambda, eta0, 1)
    var bc_last_model = sc.broadcast(last_model)

    for (epoch <- 1 to max_epoche) {
      last_model = train_a_epoch(epoch, train_set, bc_last_model)
      bc_last_model.unpersist(true)
      bc_last_model = sc.broadcast(last_model)
      // bc_last_model.destroy()
      last_model.evaluate(test_set)
    }
    last_model
  }
  def train_a_epoch(epoch: Int, rdd: RDD[(Int, Array[oneInstance])], last_model: Broadcast[LR]): LR = {
    plog(s"Train epoch [$epoch] ...")
    val scans = scan_times
    // val _depth = depth
    val _depth = 2
    val t = new Timer()
    val (model, cnt) = rdd.map(data => {
      (train_a_partition(s"$epoch-${data._1}", data._2, last_model, scans), 1)
    }).treeReduce((a, b) => (a._1.plus(b._1), a._2 + b._2), _depth)
    model.div(cnt)
    plog(s" Done epoch [$epoch] (#m=${model.size()}) (t=${t.timer()})")
    model
  }
  def train_a_partition(id: String, data: Array[oneInstance], last_model: Broadcast[LR], scan_times: Int): LR = {
    val t = new Timer()
    val model: LR = last_model.value.clone
    for (i <-0 until scan_times) {
      model.train(data, true)
    }
    plog(s"Done Partition[$id] (model: ${model.size()}) (${t.timer()})")
    model
  }
}
