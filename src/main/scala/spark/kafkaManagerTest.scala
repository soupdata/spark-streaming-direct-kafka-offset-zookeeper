package spark

import java.io.{File, PrintWriter}

import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


object kafkaManagerTest {
  def main(args: Array[String]) {
    // 怀疑可能是线程问题，于是设置成单线程,报错 Error running job streaming job 1546952615000 ms.0，于是设置成2个
    // 排除是线程的原因
    val sparkConf: SparkConf = new SparkConf().setAppName("SparkStreamingKafka_Direct").setMaster("local[4]")
    //2、创建sparkContext
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    //3、创建StreamingContext
    val ssc = new StreamingContext(sc, Seconds(5))
    //保证元数据恢复，就是Driver端挂了之后数据仍然可以恢复

    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092", "group.id" -> "Kafka_Direct","enable.auto.commit"->"false","auto.offset.reset" -> "largest","fetch.message.max.bytes" -> "20971520") //smallest largest latest
    //5、定义topic
    val topics = Set("sql-2")
    val manager = new KafkaManager(kafkaParams)
    //6、通过 KafkaUtils.createDirectStream接受kafka数据，这里采用是kafka低级api偏移量不受zk管理
    val dstreams: InputDStream[(String, String)] = manager.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    //7、获取kafka中topic中的数据
    //val topicData: DStream[String] = dstreams.map(_._2)

    //var offsetRanges = Array.empty[OffsetRange]

    println("start......")

    dstreams.foreachRDD((rdd:RDD[(String,String)]) =>{
      rdd.foreachPartition(iter=>{
      iter.foreach(x=>{
        saveData(iter)
        manager.updateZKOffsets(rdd)
      })
        println("streaming===================")
    })
    })




    //启动Spark Streaming
    ssc.start()
    //一直运行，除非人为干预再停止
    ssc.awaitTermination()
  }


//Iterator[(String,String)])
  def saveData(iterators: Iterator[(String,String)]): String ={
    // 判断指针是否指到最后去了
    var it:Iterator[(String,String)]=null
    it=iterators
    //(elem <- it) println(elem)
    var num=0
    var res: String = null
    val writer = new PrintWriter(new File("/usr/local/kafkaData.txt"))
    try {
//      println("it length:"+it.length)
//      println("it hasNext:"+it.hasNext)
//      print(it.take(7))
      // iterator type modifly
      while (it.hasNext) {
        num=num+1
        val sql = it.next()
        // val word = sql.split("")
        writer.println("data-> num-> sql"+ num+ ":"+sql)
        println("data-> num-> sql"+ num+ ":"+sql)
      }
      writer.close()
    }catch {
      case e: Exception => {
        println("save data exception:"+e)
      }
    }
    Thread.sleep(10000)
    println("saveData end")
    res
  }


}