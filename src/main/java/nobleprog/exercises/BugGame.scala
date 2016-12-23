package nobleprog.exercises

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
 * 23/12/16 WilliamZhu(allwefantasy@gmail.com)
 */
object BugGame {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("BugGame")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)

    val buffer = new ArrayBuffer[String]()

    sc.textFile("path").map(f => f.split(",")).filter(f => f.length > 1).
      map(f => buffer += f(0))

    buffer.foreach(println(_))

    sc.stop()

  }
}
