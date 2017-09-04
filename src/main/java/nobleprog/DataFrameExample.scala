package nobleprog

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 16/12/16 WilliamZhu(allwefantasy@gmail.com)
 */
object DataFrameExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("BatchSparkSQL")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = SQLContext.getOrCreate(sc)



    //解析成有格式的数据
    val words = sc.textFile("data/core/word_count.txt").flatMap(f => f.split("\\s+")).
      map(f => (f, 1)).
      reduceByKey((a, b) => a + b)

    //转化为SQL支持的Row格式的数据
    val wordCount = words.map { item =>
      Row.fromSeq(Seq(item._1, item._2))
    }


    //注册数据源，Schema
    val df = sqlContext.createDataFrame(wordCount, StructType(
      StructField("word", StringType, false) ::
        StructField("number", IntegerType, false) :: Nil
    ))

    import sqlContext.implicits._
    val ds = df.as[DC]
    println("-----dataset demo--------")
    //select sum(count) as kk from abc group by domain
    //ds.groupBy(_.word).agg(sum("number").as("kk").as[Long]).select(expr("kk").as[Long]).show(10)

    println("-----dataset2 demo--------")
    ds.select(expr("number as kk1").as[Int], expr("word as domain").as[String]).select(expr("kk1").as[Int]).show()
    ds.select(expr("number as kk1").as[Int], expr("word as kk2").as[String]).show()


    import org.apache.spark.sql.functions._
    //df.filter("age > 21");
    //df.filter(df.col("age").gt(21));
    //
    println("-----dataframe demo--------")
    //select sum(count) as c2 from table where count > 3 group by domain
    df.where("number > 3").groupBy("word").agg(sum("number") as "c2").show()

    //df转化为RDD
    df.rdd.map { f =>
      f.getAs[String]("word")
    }.collect().foreach(println(_))

    sc.stop()
  }
}

case class DC(word: String, number: Int)
