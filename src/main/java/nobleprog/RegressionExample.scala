package nobleprog

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * 12/9/16 WilliamZhu(allwefantasy@gmail.com)
 */
object RegressionExample {
   def main(args:Array[String]):Unit = {
     val conf = new SparkConf().setAppName("线性回归测试代码")
     conf.setMaster("local[2]")
     val spark  = new SparkContext(conf)

     val sqlContext = SQLContext.getOrCreate(spark)
     // Load training data
     val training = sqlContext.read.format("libsvm")
       .load("data/mllib/sample_linear_regression_data.txt")

     val lr = new LinearRegression()
       .setMaxIter(10)
       .setRegParam(0.3)
       .setElasticNetParam(0.8)
       .setTol(1E-6) //收敛阈值

     //lr.setWeightCol() 权重列，默认每个样本的权重都是1，你也可以给样本设置权重

     //lr.setSolver("auto") 设置优化函数(求解损失函数)，如果为Normal 则采用Normal Equation (正规化方程)
     // 如果为auto,则判定如果     elasticNetParam==0 && numFeatures < 4096 则也选用Normal Equation (正规化方程)
     // 如果选用正规化方程，会默认使用 L2 正则化向，并且采用WeightedLeastSquares
     // 否则L-BFGS(Limited-Memory BFGS)是BFGS算法在受限内存时的一种近似算法，而BFGS是数学优化中一种无约束最优化算法

     //lr.setStandardization(true) 是否对特征进行标准化，比如映射到0-1空间等，典型如使用sigma 函数

     // Fit the model
     val lrModel = lr.fit(training)

     // Print the coefficients and intercept for linear regression
     println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

     // Summarize the model over the training set and print out some metrics
     val trainingSummary = lrModel.summary
     println(s"numIterations: ${trainingSummary.totalIterations}")
     println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")
     trainingSummary.residuals.show()
     println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
     println(s"r2: ${trainingSummary.r2}")

   }
}
