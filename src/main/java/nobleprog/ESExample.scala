package nobleprog

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark

/**
 * 16/12/16 WilliamZhu(allwefantasy@gmail.com)
 */
object ESExample {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().appName("ESExample").
      master("local[2]").
      getOrCreate()



    //RDD API
    val words = sparkSession.sparkContext.
      textFile("data/core/word_count.txt").
      map(f => Map("a" -> f))

    val cfg = Map(
      "es.resource" -> "test/test",
      "es.nodes" -> "127.0.0.1"
    )
    EsSpark.saveToEs(words, cfg)


    //DataFrame API

    //解析成有格式的数据
    val words2 = sparkSession.sparkContext.textFile("data/core/word_count.txt").flatMap(f => f.split("\\s+")).
      map(f => (f, 1)).
      reduceByKey((a, b) => a + b)

    //转化为SQL支持的Row格式的数据
    val wordCount = words2.map { item =>
      Row.fromSeq(Seq(item._1, item._2))
    }

    //注册数据源，Schema
    val df = sparkSession.sqlContext.createDataFrame(wordCount, StructType(
      StructField("word", StringType, false) ::
        StructField("number", IntegerType, false) :: Nil
    ))

    df.write.
      format("org.elasticsearch.spark.sql").
      options(cfg).mode(SaveMode.Overwrite).
      save()

    //读取数据
    val dfRead = sparkSession.sqlContext.read.format("org.elasticsearch.spark.sql").
      options(cfg).load("test/test")

    dfRead.createOrReplaceTempView("test")

    sparkSession.sqlContext.sql("select * from test").write.csv("/tmp/es-test2")
    sparkSession.stop()
  }

}
