package nobleprog

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 16/12/16 WilliamZhu(allwefantasy@gmail.com)
  */
object FirstSStreaming {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("我们的第一个Spark Structured streaming程序").
      master("local[2]").
      getOrCreate()

    import spark.implicits._

    val ss = spark.readStream.format("socket")
      .option("host", "localhost")
      .option("port", 9005)
      .load()

    val query = ss.as[String].flatMap(_.split(" ")).writeStream
      .outputMode("append")
      .format("console")
      .start()

    query.awaitTermination()
  }

}
