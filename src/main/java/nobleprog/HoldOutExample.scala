package nobleprog

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 16/12/16 WilliamZhu(allwefantasy@gmail.com)
 */
object HoldOutExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("贝叶斯测试代码")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)

    val sampleLdaData = sc.textFile("data/core/mllib/sample_lda_data.txt")
    sampleLdaData.takeSample(false,2).foreach(println(_))
  }
}
