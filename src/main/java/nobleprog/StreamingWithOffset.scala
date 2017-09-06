package nobleprog

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 16/12/16 WilliamZhu(allwefantasy@gmail.com)
  * /usr/local/Cellar/kafka/0.10.1.1
  * ./bin/kafka-console-producer --broker-list localhost:9092 --topic test2
  * brew services start  kafka
  */
object StreamingWithOffset {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("测试StreamingSQL应用")
    val duration = 5
    conf.setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(duration))
    //ssc.addStreamingListener(new BatchStreamingListener(ssc.sparkContext))
    val kafkaParams = Map("metadata.broker.list" -> "127.0.0.1:9092")
    KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc, kafkaParams, Set("test2")).transform{ rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      for (o <- offsetRanges) {
        println(
          s"topic: ${o.topic}, partition: ${o.partition}, fromOffset: ${o.fromOffset}, untilOffset:" +
            s" ${o.untilOffset}, length: ${o.untilOffset - o.fromOffset}")
      }
      rdd
    }.map(f => f).foreachRDD { rdd =>
      println(s"周期内容数： ${rdd.count()}")
    }

    ssc.start()
    ssc.awaitTermination()
  }

}

//class BatchStreamingListener(context: SparkContext) extends StreamingListener {
//
//  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
//    val time = batchCompleted.batchInfo.batchTime
//    print("保存offset文件")
//
//  }
//
//}
