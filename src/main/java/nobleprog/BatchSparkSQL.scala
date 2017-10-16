package nobleprog

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * 16/12/16 WilliamZhu(allwefantasy@gmail.com)
  */
object BatchSparkSQL {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().
      appName("batch-spark-sql").
      master("local[2]").
      getOrCreate()

    val holder = new ArrayBuffer[String]()
    //解析成有格式的数据
    val words = spark.sparkContext.textFile("data/core/word_count.txt").
      flatMap(f => f.split("\\s+")).
      map { f =>
        holder += f
        (f, 1)

      }

    //转化为SQL支持的Row格式的数据
    val wordCount = words.map { item =>
      Row.fromSeq(Seq(item._1, item._2))
    }

    //注册数据源，Schema
    val df = spark.createDataFrame(wordCount, StructType(
      StructField("word", StringType, false) ::
        StructField("number", IntegerType, false) :: Nil
    ))

    //注册表名
    df.createOrReplaceTempView("words")

    spark.udf.register("mkString",
      (sep: String, co: mutable.WrappedArray[String]) => {
      co.mkString(sep)
    })


    //使用SQL查询
    val df1 = spark.sql("select word,mkString(',',collect_list(number)) as countNum from words group by word")
    df1.show(100, false)

    spark.stop()
  }

}
