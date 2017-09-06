package nobleprog

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
 * 16/12/16 WilliamZhu(allwefantasy@gmail.com)
 */
object BatchSparkSQL {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("BatchSparkSQL")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)

    val sqlContext = SQLContext.getOrCreate(sc)

    val holder = new ArrayBuffer[String]()
    //解析成有格式的数据
    val words = sc.textFile("data/core/word_count.txt").
      flatMap(f => f.split("\\s+")).
      map{f =>
      holder += f
      (f, 1)

    }

    //转化为SQL支持的Row格式的数据
    val wordCount = words.map { item =>
      Row.fromSeq(Seq(item._1, item._2))
    }

    //注册数据源，Schema
    val df = sqlContext.createDataFrame(wordCount, StructType(
      StructField("word", StringType, false) ::
        StructField("number", IntegerType, false) :: Nil
    ))

    //注册表名
    df.createOrReplaceTempView("words")

    //使用SQL查询
    df.sqlContext.sql("select word,sum(number) as countNum from words group by word").show()

    sc.stop()
  }

}
