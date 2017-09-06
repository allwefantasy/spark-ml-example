package nobleprog

import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.streaming.{Seconds, StreamingContext, TestInputStream}

/**
 * 16/12/16 WilliamZhu(allwefantasy@gmail.com)
 */
object FirstStreaming {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("测试StreamingSQL应用")
    val duration = 5
    conf.setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(duration))

    ssc.socketTextStream("127.0.0.1",9005).map(f=>f).foreachRDD{rdd=>
        println(s"周期内容数： ${rdd.count()}")
    }


    ssc.start()
    ssc.awaitTermination()
  }

}
