package exmaple

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext, TestInputStream}

/**
  * Created by allwefantasy on 11/2/2017.
  */
object BatchJobTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("test")
    conf.setMaster("local[4]")
    conf.set("spark.scheduler.mode", "FAIR")
    val sc = new SparkContext(conf)


    val input = sc.parallelize(Seq(1, 2, 3), 1)
    val input2 = sc.parallelize(Seq(1, 2, 3), 1)


    input.map { f =>
      Thread.sleep(5000)
      f
    }.count()

    input2.map { f =>
      Thread.sleep(5000)
      f
    }.count()

    Thread.sleep(100000)
    sc.stop()

  }
}
