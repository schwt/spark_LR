package fp_growth
import org.apache.spark.{SparkConf, SparkContext}
// import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.fpm.FPGrowth
import sgd4spark.utils.utils.plog
import sgd4spark.utils.Timer

object train {

  def main(args: Array[String]): Unit = {
    plog("start...")
    plog(s"# args: ${args.length}")
    if (args.length < 2) {
      System.err.println("Error arguments!")
      System.exit(1)
    }
    val input_path  = args(0)
    val output_path = args(1)
    val min_support = args(2).toDouble

    val spark_conf = new SparkConf()
      // .set("spark.driver.maxResultSize", "0")
      // .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
    spark_conf.getAll.foreach{ p => plog(s"\tSpark Conf: ${p._1} = ${p._2}")}
    val sc = new SparkContext(spark_conf)
    sc.hadoopConfiguration.setBoolean("mapred.output.compress", false)

    val data = sc.textFile(input_path)
      .map(s => s.trim.split(","))
      .filter(x => x.length > 3)
    plog(s"# input data: ${data.count()}")

    plog("start...")
    val timer = new Timer
    val fpg = new FPGrowth()
      .setMinSupport(min_support)
    val model = fpg.run(data)
    plog(s"train time: ${timer.timer()}")

    val result1 = model.freqItemsets.filter{itemset => itemset.items.length > 1}
      .map{itemset  => itemset.freq + ":" + itemset.items.mkString("[", ",", "]")}

    val result2 = model.generateAssociationRules(0.5).map{ rule =>
      rule.confidence + ":" +
      rule.antecedent.mkString("[", ",", "]") + "=>" +
      rule.consequent.mkString("[", ",", "]")
    }
    plog(s"# output1 size: ${result1.count()}")
    plog(s"# output2 size: ${result2.count()}")

    result1.coalesce(1).saveAsTextFile(output_path)
    result2.coalesce(1).saveAsTextFile(output_path+"_2")
    plog(s"save done")
    sc.stop()
  }
  /*
  def load_data(sc: SparkContext, path: String): RDD[Array[String]] = {
    plog("load data...")
  }
  */

}
