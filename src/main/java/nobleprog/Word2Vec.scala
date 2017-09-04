package nobleprog

import exmaple.Jack
import org.ansj.splitWord.analysis.NlpAnalysis
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}

import scala.collection.JavaConversions._

/**
  * Created by allwefantasy on 17/5/2017.
  */
object Word2Vec {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("BatchSparkSQL")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)


    val words = sc.textFile("file:///Users/allwefantasy/Downloads/results-2.csv")

    val input = words.filter(f => !f.isEmpty).map { line =>
      NlpAnalysis.parse(line).getTerms.map(f => f.getName).toSeq
    }

    val word2vec = new Word2Vec()

    val model = word2vec.fit(input)

    val result = model.getVectors.map(f =>s""" ${f._1} ${f._2.mkString(",")} """)
    sc.parallelize(result.toSeq, 1).saveAsTextFile("file:///tmp/laiwenvector")
    //    val synonyms = model.findSynonyms("化验", 5)
    //
    //    for((synonym, cosineSimilarity) <- synonyms) {
    //      println(s"$synonym $cosineSimilarity")
    //    }
    //
    //    // Save and load model
    //    model.save(sc, "file:///tmp/myModelPath")
    //    //val sameModel = Word2VecModel.load(sc, "myModelPath")
    sc.stop()

  }
}

object Anaysis {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("BatchSparkSQL")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)


    val words = sc.textFile("file:///Users/allwefantasy/Downloads/results-2.csv")

    val input = words.mapPartitions { f =>
      val jack = new Jack()
      f.map(k => jack.echo(k))
    }
    input.saveAsTextFile("file:///tmp/laiwenwords1")
    sc.stop()

  }
}
