package nobleprog.exercises

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * 23/12/16 WilliamZhu(allwefantasy@gmail.com)
 */
object FileCollect {
  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("BugGame")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)


    /*
     /tmp/file_collect 文件内容：

     v1,bj,message1
     v1,tj,message2

     */
    val content = sc.textFile("file:///tmp/file_collect").map { f =>
      val Array(a, b, c) = f.split(",")
      (b, c)
    }

    val cities = content.map(f => f._1).distinct().collect()
    val numCities = cities.length
    val cityToNumber = sc.broadcast(cities.zipWithIndex.toMap)
    val numberToCities = sc.broadcast(cities.zipWithIndex.map(f => (f._2, f._1)).toMap)

    content.partitionBy(new Partitioner {
      override def numPartitions: Int = numCities

      override def getPartition(key: Any): Int = {
        cityToNumber.value(key.toString)
      }
    }).mapPartitionsWithIndex { (partitonNumber, iter) =>
      saveFile("file:///tmp", numberToCities.value(partitonNumber), iter)
      iter
    }.count()

    def saveFile(path: String, fileName: String, iterator: Iterator[(String, String)]) = {

      var dos: FSDataOutputStream = null
      try {

        val fs = FileSystem.get(new Configuration())
        if (!fs.exists(new Path(path))) {
          fs.mkdirs(new Path(path))
        }
        dos = fs.create(new Path(path + s"/$fileName"), true)
        iterator.foreach { x =>
          dos.writeBytes(x._2 + "\n")
        }
      } catch {
        case ex: Exception =>
          println("file save exception")
      } finally {
        if (null != dos) {
          try {
            dos.close()
          } catch {
            case ex: Exception =>
              println("close exception")
          }
          dos.close()
        }
      }

    }

    sc.stop()
  }
}
