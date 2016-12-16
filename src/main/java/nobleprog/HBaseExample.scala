package nobleprog


import org.apache.spark.{SparkConf, SparkContext}

/**
 * 16/12/16 WilliamZhu(allwefantasy@gmail.com)
 */
object HBaseExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("BatchSparkSQL")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)

    val words = sc.textFile("data/core/word_count.txt")

    words.foreachPartition { f =>
//      SimpleHBaseClient.createTableIfNotExists("wiki")
      f.foreach { line =>
        //RowKey规则: md5+";"+datetime+";"+realkey
        val key = System.currentTimeMillis() + ""
        SimpleHBaseClient.put(key, line)
        println(SimpleHBaseClient.get(key))
      }
    }
  }
}
