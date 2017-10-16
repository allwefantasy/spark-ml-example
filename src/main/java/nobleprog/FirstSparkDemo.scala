package nobleprog

import org.apache.spark.sql.SparkSession

/**
  * 12/13/16 WilliamZhu(allwefantasy@gmail.com)
  */
object FirstSparkDemo {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("我们的第一个Spark程序").
      master("local[2]").
      getOrCreate()

    val words = sparkSession.sparkContext.textFile("/Users/allwefantasy/CSDNWorkSpace/spark-ml-example/data/core/word_count.txt")
    val dict = Map("wow"->"1","wow1"->"2")
    val newDict = sparkSession.sparkContext.broadcast(dict)
    val wordCount = words.flatMap(f =>
      f.split("\\s+")).map {
      f =>
        //Thread.sleep(1000*100)
        newDict.value("wow")
        (f, 1)
    }.
      reduceByKey {
        (a, b) => a + b
      }.map {
      f => (f._2, f._1)
    }.sortByKey(false, 1)

    wordCount.map(f => s"${f._1}->${f._2}").repartition(3).saveAsTextFile("file:///tmp/4")
    sparkSession.stop()
  }
}
