package spark

import java.io.{File, PrintWriter}

import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


object kafkaManagerTest {
  def main(args: Array[String]) {

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


//    val transformRdd=dstreams
//      .transform {
//        rdd =>
//          //rdd.foreach(x=>println("transform rdd:"+x))
//          //offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//          manager.updateZKOffsets(rdd)
//
//          rdd
//      }
//    val mapRdd=transformRdd.map(x=>{
//      println("maprdd 1:"+x._1)
//      println("maprdd 2:"+x._2)
//      x._2
//    })
//    mapRdd.foreachRDD((rdd: RDD[String]) => {
//      //rdd.foreach(result=>println("foreachRdd result:"+result))
//        rdd.foreachPartition(iter=>
//          iter.foreach(record=> {
//            println("recode:"+record)
//            saveData(iter)
//          })
//        )
//    })
    println("start......")

    dstreams.foreachRDD((rdd:RDD[(String,String)]) =>{
      rdd.foreachPartition(iter=>{
      iter.foreach(x=>saveData(iter))
      manager.updateZKOffsets(rdd)
        println("executor update offset")
    })
    })



    //启动Spark Streaming
    ssc.start()
    //一直运行，除非人为干预再停止
    ssc.awaitTermination()
  }



  def saveData(iterator: Iterator[(String,String)]): String ={
    var num=0
    var res: String = null
    val writer = new PrintWriter(new File("/usr/local/kafkaData.txt"))
    try {
      println("iterator length:"+iterator.length)
      iterator.foreach(x=>println("x->"+x))
      println("iterator.hasNext:"+iterator.hasNext)
      // iterator type modifly
      while (iterator.hasNext) {
        num=num+1
        val sql = iterator.next()
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