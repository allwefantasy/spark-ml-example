package nobleprog

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.streaming.{TestInputStream, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 16/12/16 WilliamZhu(allwefantasy@gmail.com)
 */
object StreamingSparkSQL {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("测试StreamingSQL应用")
    val isDebug = true
    val duration = 5
    if (isDebug) {
      conf.setMaster("local[2]")
    }
    val ssc = new StreamingContext(conf, Seconds(duration))

    val source = ssc.sparkContext.textFile("data/core/word_count.txt").flatMap(f => f.split("\\s+")).
      map(f => (f, 1)).
      reduceByKey((a, b) => a + b).map(f=>s"${f._1} ${f._2}").collect().toSeq

    val data = new TestInputStream[String](ssc, Seq(source), 1)

    data.map(f=>f.split("\\s+")).map(a=>(a(0),a(1).toInt)).foreachRDD{rdd=>

      //注册数据源，Schema
      val df = SQLContext.getOrCreate(rdd.context).createDataFrame(rdd.map { item =>
        Row.fromSeq(Seq(item._1, item._2))
      }, StructType(
        StructField("word", StringType, false) ::
          StructField("number", IntegerType, false) :: Nil
      ))

      //注册表名
      df.createOrReplaceTempView("words")

      //使用SQL查询
      df.sqlContext.sql("select * from words").show()
    }


    ssc.start()
    ssc.awaitTermination()
  }

}
