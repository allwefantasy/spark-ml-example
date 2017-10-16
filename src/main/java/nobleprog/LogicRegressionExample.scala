package nobleprog

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.{SparkSession}

/**
 * 12/9/16 WilliamZhu(allwefantasy@gmail.com)
 */
object LogicRegressionExample {
   def main(args:Array[String]):Unit = {

     val sparkSession = SparkSession.builder().appName("逻辑回归测试代码").master("local[2]").getOrCreate()

     // Load training data
     val training = sparkSession.read.format("libsvm")
       .load("data/mllib/sample_libsvm_data.txt")

     val mlr = new LogisticRegression()
       .setMaxIter(10)
       .setRegParam(0.3)
       .setElasticNetParam(0.8)
       .setFamily("multinomial")


     //lr.setWeightCol() 权重列，默认每个样本的权重都是1，你也可以给样本设置权重
     //lr.setStandardization(true) 是否对特征进行标准化，比如映射到0-1空间等，典型如使用sigma 函数

     // Fit the model
     val lrModel = mlr.fit(training)

     lrModel.save("/tmp/lr")
     sparkSession.stop()
   }
}
