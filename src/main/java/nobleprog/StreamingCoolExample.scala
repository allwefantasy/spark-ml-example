package nobleprog

import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.{Partitioner, SparkConf}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by allwefantasy on 6/9/2017.
  * /usr/local/Cellar/kafka/0.10.1.1
  */
object StreamingCoolExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("测试StreamingSQL应用")
    val duration = 10
    conf.setMaster("local[2]")
    //控制速率
    conf.set("spark.streaming.kafka.maxRatePerPartition", "3000")

    val ssc = new StreamingContext(conf, Seconds(duration))

    def createContext() = {
      val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,

        Map("metadata.broker.list" -> "127.0.0.1:9092",
          "fetch.message.max.bytes" -> "10485760",
          "replica.fetch.max.bytes" -> "10485760",
          "message.max.bytes" -> "10485760",
          "auto.offset.reset" -> "largest"),
        Set("wow")
      )
      kafkaStream.map { f =>
        val Array(client, log, year, month, day, hour) = f._2.split(",")
        (s"/tmp/userlog/$client/$year/$month/$day/$hour", log)
      }.foreachRDD { (rdd,duratioinTime) =>
        val paths = rdd.map(f => f._1).collect()
        val pathToIndexDis = rdd.context.broadcast(paths.zipWithIndex.toMap)
        val indexToPathDis = rdd.context.broadcast(paths.zipWithIndex.map(f => (f._2, f._1)).toMap)
        rdd.partitionBy(new Partitioner {
          override def numPartitions: Int = pathToIndexDis.value.size

          override def getPartition(key: Any): Int = pathToIndexDis.value(key.toString)
        }).mapPartitionsWithIndex((index, partitionData) => {
          val path = indexToPathDis.value(index)
          val fileName = duratioinTime.milliseconds + "_" + index
          HDFSUtil.saveFile(path,fileName,partitionData)
          partitionData
        }, true
        ).count()
      }
    }
    createContext()

    ssc.start()
    ssc.awaitTermination()
  }
}
