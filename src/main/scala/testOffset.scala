import java.io.{File, PrintWriter}

import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}

object testOffset {
  def main(args: Array[String]) {
    def main(args: Array[String]) {
      //=====================================================================

      //=====================================================================
      var count = 0
      val sparkConf: SparkConf = new SparkConf().setAppName("SparkStreamingKafka_Direct").setMaster("local[2]")
      //2、创建sparkContext
      val sc = new SparkContext(sparkConf)
      sc.setLogLevel("WARN")
      //3、创建StreamingContext
      val ssc = new StreamingContext(sc, Seconds(5))
      //保证元数据恢复，就是Driver端挂了之后数据仍然可以恢复

      val kafkaParams = Map("metadata.broker.list" -> "localhost:9092", "group.id" -> "Kafka_Direct", "auto.offset.reset" -> "largest", "enable.auto.commit" -> "false", "fetch.message.max.bytes" -> "20971520") //smallest largest latest
      //5、定义topic
      val topics = Set("sql-2")
      //    val manager = new KafkaManagers(kafkaParams)
      //    //6、通过 KafkaUtils.createDirectStream接受kafka数据，这里采用是kafka低级api偏移量不受zk管理
      val dstreams: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
      //
      //    //7、获取kafka中topic中的数据
      val topicData: DStream[String] = dstreams.map(_._2)
      //    // 更新offsets
      //    manager.updateZKOffsets(rdd)


      /**
        * foreachPartition action
        */
      topicData.foreachRDD((rdd: RDD[String]) => {
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        // some time later, after outputs have completed
        println(offsetRanges)
        try {
          count = count + 1
          println(count + "rdd-action")
          // println("输出分区数："+TaskContext.get.partitionId)
          rdd.foreachPartition(iter =>
            iter.foreach(record => parseSql(iter))

          )
          println("rdd-end")
        } catch {
          case e: Exception => println("exception" + e)
        }

      })




      //启动Spark Streaming
      ssc.start()
      //一直运行，除非人为干预再停止
      ssc.awaitTermination()
    }

    def parseSql(iterator: Iterator[String]): Iterator[String] = {
      var num = 0
      var res: Iterator[String] = null
      try {
        while (iterator.hasNext) {
          num = num + 1
          val sql = iterator.next()
          val word = sql.split("")
          // Thread.sleep(200)
          println("sql:" + num + "->" + sql)
        }
      } catch {
        case e: Exception => {
          println("parseSql" + e)
        }
      }
      //Thread.sleep(12000)
      println("end")
      res
    }


    def parseSqlString(iterator: Iterator[String]): String = {
      var num = 0
      var res: String = null
      val writer = new PrintWriter(new File("/usr/local/kafkaData.txt"))
      try {
        while (iterator.hasNext) {
          num = num + 1
          val sql = iterator.next()
          val word = sql.split("")
          // Thread.sleep(200)
          writer.println("sql:" + num + "->" + sql)
          println("sql:" + num + "->" + sql)
        }
        writer.close()
      } catch {
        case e: Exception => {
          println("parseSql" + e)
        }
      }
      //Thread.sleep(12000)
      println("end")
      res
    }

  }



  }