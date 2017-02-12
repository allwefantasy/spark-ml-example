package nobleprog


import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

/**
 * 12/10/16 WilliamZhu(allwefantasy@gmail.com)
 */
object NaiveBayesExample {
  def main(args:Array[String]):Unit = {

    val conf = new SparkConf().setAppName("贝叶斯测试代码")
    conf.setMaster("local[2]")
    val spark  = new SparkContext(conf)

    val data = spark.textFile("data/mllib/sample_naive_bayes_data.txt")
    val parsedData = data.map { line =>
      val parts = line.split(',')
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }

    // Split data into training (60%) and test (40%).
    val splits = parsedData.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0)
    val test = splits(1)

    //这里有一个核心配置，如果是连续值，
    // 比如tf/idf 则建议使用Multinomial，
    // 如果feature 都是0/1 则使用
    //Bernoulli
    val model = NaiveBayes.train(training, lambda = 1.0, modelType = "multinomial")

    val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
    println(s"accuracy => "+accuracy)
    // Save and load model
    //model.save(spark, "target/tmp/myNaiveBayesModel")
    //val sameModel = NaiveBayesModel.load(spark, "target/tmp/myNaiveBayesModel")
  }
}
