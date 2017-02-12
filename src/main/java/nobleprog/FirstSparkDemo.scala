package nobleprog

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 12/13/16 WilliamZhu(allwefantasy@gmail.com)
 */
object FirstSparkDemo {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("测试Streaming应用")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    val words = sc.textFile("data/core/word_count.txt")

    val wordCount = words.flatMap(f => f.split("\\s+")).
      map{
      f =>
        (f, 1)
    }.reduceByKey((a, b) => a + b).map(f=>(f._2,f._1)).sortByKey(false,1)

    wordCount.map(f=>s"${f._1}->${f._2}").saveAsTextFile("file:///tmp/2")
    sc.stop()
  }
}
