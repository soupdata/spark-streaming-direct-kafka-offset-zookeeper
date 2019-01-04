import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

object helloworld {
  var count=0
  def main(args: Array[String]) {

    //创建SparkConf对象
    val sparkConf = new SparkConf().setAppName("HdfsWordCount").setMaster("local[2]")
    // Create the context
    //创建StreamingContext对象，与集群进行交互
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //如果目录中有新创建的文件，则读取
    val lines = ssc.textFileStream("file:///usr/local/sofa.sql")
    lines.foreachRDD((rdd: RDD[String], time: Time) => {
      try {
        count = count + 1
        println(count + "rdd")
        val dstream = rdd.mapPartitions(k => parseSql(k))
        dstream.foreach(x => println())
      }catch {
        case e:Exception=>println("exception"+e)
      }

    })
    //启动Spark Streaming
    ssc.start()

    //一直运行，除非人为干预再停止
    ssc.awaitTermination()
  }

  def parseSql(iterator: Iterator[String]): Iterator[String] ={
    var num=0
    var res: Iterator[String] = null
    try {
      while (iterator.hasNext) {
        num=num+1
        val sql = iterator.next()
        val word = sql.split("")
        println("sql:"+ num+ "->"+sql)
      }
    }catch {
      case e: Exception => {
        println("parseSql"+e)
      }
    }
    Thread.sleep(300000)
    println("end")
    res
  }
}