package nobleprog

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * 12/9/16 WilliamZhu(allwefantasy@gmail.com)
 */
object RegressionTest {
   def main(args:Array[String]):Unit = {
     val conf = new SparkConf().setAppName("逻辑回归测试代码")
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
