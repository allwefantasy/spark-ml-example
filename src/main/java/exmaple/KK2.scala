package exmaple


import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.JavaConversions._


object ItemSimilarity {


  /**
    * 得到参数传递进来的所有组合。
    * @param list list(1,2,3)
    * @return  （1,2） （1，3）（2，3）
    */
  def ppCount(list: Seq[Int]) = {
    val result = new mutable.ListBuffer[Seq[Int]]
    for (i <- 0 to list.length - 1; j <- (i + 1) to list.length - 1) result += Seq(list(i), list(j))
    result
  }

  /**
    * 单线程reduce
    * @param collection  [(1,2),(1,3),(2,3)]
    * @param num
    * @tparam K
    * @tparam V
    * @return  [(1,5),(2,3)]
    */
  def ppReduceByKey[K, V](collection: Traversable[Tuple2[K, V]])(implicit num: Numeric[V]) = {
    import num._
    collection
      .groupBy(_._1)
      .map {
        case (group: K, traversable) => traversable.reduce {
          (a, b) => (a._1, a._2 + b._2)
        }
      }
  }

  /**
    * 参看 ppReduceByKey
    * @param list
    * @return
    */
  def reduceWow(list: Seq[(Int, Int)]) = {
    ppReduceByKey(list).toArray
  }

  /**
    * 检查数据格式
    * @param term
    * @return
    */
  def check(term: (String, String)) = {
    try {
      term._2.toInt
      term._1.toInt
      true
    } catch {
      case e: Exception => false
    }
  }


  /**
    * 相似度计算公式
    * @param ab  a,b共同出现次数
    * @param a   a单独出现次数
    * @param b   b单独出现次数
    * @return    相应的概率
    */
  def compute(ab: Int, a: Int, b: Int): Double = {
    return ab / Math.sqrt(Math.pow(a, 2) + Math.pow(b, 2))
  }




  def main(args: Array[String]) = {
    val spark = SparkSession.builder().appName("ItemBased").
      master("local[2]").
      getOrCreate()

    val ori = spark.sparkContext.textFile("hdfs://csdn/output/debug/user_click_trainging_file_small/").map(_.split(" ") match {
      case Array(user, item, pref) => (user.toInt, item.toInt)
    })

    /*
      每个元素出现的次数
     */
    val itemCount = ori.map {
      case (user, item) => (item, 1)
    }.reduceByKey(_ + _).collect().toMap[Int, Int]

    /*
      把这个map对象发布到每个计算节点的进程里
     */
    val ic = spark.sparkContext.broadcast(itemCount)

    /*
       下面的代码可以存储结果到redis集群中去的。如果使用pipeline 应该更高效
     */
    val result = ori.groupByKey(100). //user => item1 item2 item3
      flatMap(term => ppCount(term._2.toSet.toSeq)). //(item1,item2)
      map(pair => (pair, 1)). //((item1,item2),1)
      reduceByKey(_ + _). //((item1,item2),?)
      flatMap {
      term =>
        val item1 = (term._1(0), (term._1(1), term._2)) //(item1,(item2,?))
      val item2 = (term._1(1), (term._1(0), term._2)) //(item2,(item1,?))
        List(item1, item2)
    }.groupByKey(100). //此时格式应该是 item1 => (item2,?) (item3,?) (item4,?)
      map {
      term =>
        val id = term._1
        val sim = reduceWow(term._2.toSeq).map {
          item => (item._1, compute(item._2, ic.value(term._1), ic.value(item._1)))
        }
        (id, sim)
    }
    result.foreachPartition {
      rdd =>
//        import com.redis._
//        val r = new RedisClient("localhost", 6379)
//        rdd.foreach {
//          case (id, sim) => sim.map(i => r.zadd(s"0:$id", i._2, i._1))
//        }
//        r.disconnect
    }
  }

}
