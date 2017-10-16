package nobleprog

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ForeachWriter, SparkSession}
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

    val writeStream = ss.as[String].flatMap(_.split(" ")).writeStream

    writeStream.foreach(new ForeachWriter[String] {
      override def process(value: String): Unit = {
        //对分区的每个处理
      }

      override def close(errorOrNull: Throwable): Unit = {
        //不保证一定被调用
      }

      override def open(partitionId: Long, version: Long): Boolean = {
        //每个 partition 即每个 task 会在开始时调用此 open() 方法,
        // 你可以通过版本version字段判定这个是不是需要重做
        //虽然SS保证内部的幂等性，
        // 但是只要有数据同步到外部，就会存在多次写入的问题。
        // 如果你通过version和partitionId 已经发现内容
        //被写过了，则可以返回false 跳过这次写入

        true
      }
    })

    val query = writeStream.outputMode("append")
      .format("console")
      .start()

    query.awaitTermination()
  }

}
