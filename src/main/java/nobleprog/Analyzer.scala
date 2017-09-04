package nobleprog

import org.ansj.splitWord.analysis.NlpAnalysis
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConversions._

/**
  * Created by allwefantasy on 17/5/2017.
  */
object Analyzer {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("BatchSparkSQL")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    val input = sc.textFile("/Users/allwefantasy/Downloads/results-2.csv")
    input.filter(f => !f.isEmpty).map { line =>
      NlpAnalysis.parse(line).getTerms.map(f => f.getName).mkString(" ")
    }
  }
}
