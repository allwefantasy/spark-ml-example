package nobleprog

import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 12/11/16 WilliamZhu(allwefantasy@gmail.com)
 */
object KNNTest {
  def main(args: Array[String]): Unit = {

//    val conf = new SparkConf().setAppName("贝叶斯测试代码")
//    conf.setMaster("local[2]")
//    val spark = new SparkContext(conf)
//
//    val training = MLUtils.loadLibSVMFile(spark, "data/mllib/sample_libsvm_data.txt").toDF()
//
//    val knn = new KNNClassifier()
//      .setTopTreeSize(training.count().toInt / 500)
//      .setK(10)
//
//    val knnModel = knn.fit(training)
//
//    val predicted = knn.transform(training)
  }

}
