package exmaple

import org.apache.spark.sql.SparkSession


import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS

/**
  * Created by allwefantasy on 21/9/2017.
  */
object KK {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("我们的第一个Spark Structured streaming程序").
      master("local[2]").
      getOrCreate()

    import spark.implicits._

    case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)
    def parseRating(str: String): Rating = {
      val fields = str.split("::")
      assert(fields.size == 4)
      Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
    }

    val ratings = spark.read.textFile("data/mllib/als/sample_movielens_ratings.txt")
      .map(parseRating)
      .toDF()
    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))

    // Build the recommendation model using ALS on the training data
    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
    val model = als.fit(training)
    val movieRecs = model.recommendForAllItems(10)

  }
}
