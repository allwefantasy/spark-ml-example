package nobleprog

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 12/9/16 WilliamZhu(allwefantasy@gmail.com)
 */
object LogicRegressionExample {
   def main(args:Array[String]):Unit = {
     val conf = new SparkConf().setAppName("逻辑回归测试代码")
     conf.setMaster("local[2]")
     val spark  = new SparkContext(conf)

     val sqlContext = SQLContext.getOrCreate(spark)
     // Load training data
     val training = sqlContext.read.format("libsvm")
       .load("data/mllib/sample_linear_regression_data.txt")

     val lr = new LogisticRegression()
       .setMaxIter(10)
       .setRegParam(0.3)
       .setElasticNetParam(0.8)
       .setTol(1E-6) //收敛阈值


     //lr.setWeightCol() 权重列，默认每个样本的权重都是1，你也可以给样本设置权重
     //lr.setStandardization(true) 是否对特征进行标准化，比如映射到0-1空间等，典型如使用sigma 函数

     // Fit the model
     val lrModel = lr.fit(training)

     // Print the coefficients and intercept for linear regression
     println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

     // Summarize the model over the training set and print out some metrics
     val trainingSummary = lrModel.summary
     println(s"numIterations: ${trainingSummary.totalIterations}")
     println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")

   }
}
