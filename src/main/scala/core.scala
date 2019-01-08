import java.io.{File, PrintWriter}

import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
/**
  * KafkaUtils的createDirectStream()方式，此方法直接从kafka的broker的分区中读取数据，跳过了zookeeper，并且没有receiver，
  *是spark的task直接对接kakfa topic partition，能保证消息恰好一次语意，但是此种方式因为没有经过zk，topic的offset也就没有保存，
  *当job重启后只能从最新的offset开始消费消息，造成重启过程中的消息丢失。
  *
  * kafka acks
  * 0 只要发送出去就表示成功
  * 1 把数据发送到分区返回响应成功或失败
  * all 最安全但是耗时，等到所有的副本都同步响应才确认数据发送成功
  *
  * 最多一次At-most-once：客户端收到消息后，在处理消息前自动提交，这样kafka就认为consumer已经消费过了，偏移量增加。
  *                     设置enable.auto.commit为ture
                        设置 auto.commit.interval.ms为一个较小的时间间隔.
                        client不要调用commitSync()，kafka在特定的时间间隔内自动提交即可。
  * 最少一次At-least-once：客户端收到消息，处理消息，再提交反馈。这样就可能出现消息处理完了，在提交反馈前，网络中断或者程序挂了，那么kafka认为这个消息还没有被consumer消费，产生重复消息推送
                        method1：设置enable.auto.commit为false
                                client调用commitSync()，增加消息偏移;
                                consumer.commitAsync(); //提交offset
                        method2：设置enable.auto.commit为ture
                                设置 auto.commit.interval.ms为一个较大的时间间隔->auto.commit.interval.ms", "99999999"
                                client调用commitSync(),增加消息偏移
                                consumer.commitAsync(); //提交offset
  * 正好一次Exactly-once：保证消息处理和提交反馈在同一个事务中，即有原子性。
                        如果要实现这种方式，必须自己控制消息的offset，自己记录一下当前的offset，对消息的处理和offset的移动必须保持在同一个事务中，
                        例如在同一个事务中，把消息处理的结果存到mysql数据库（或者zookeeper中）同时更新此时的消息的偏移。

  */
object core {
  def main(args: Array[String]) {
    var count=0
    val sparkConf: SparkConf = new SparkConf().setAppName("SparkStreamingKafka_Direct").setMaster("local[2]")
    //2、创建sparkContext
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    //3、创建StreamingContext
    val ssc = new StreamingContext(sc, Seconds(5))
    //保证元数据恢复，就是Driver端挂了之后数据仍然可以恢复
    /**
      *而 Direct API 方法是通过 Kafka 低层次的 API，并没有使用到 Zookeeper，偏移量仅仅被 Spark Streaming 保存在 Checkpoint 中。
      *这就消除了 Spark Streaming 和 Zookeeper 中偏移量的不一致，而且可以保证每个记录仅仅被 Spark Streaming 读取一次，即使是出现故障。
      *但是这样还是有弊端：
      * checkpoint出现问题，反序列化异常代码更新等等，以下说明的很清楚
      * 现在要从 topic 的 partition 哪些位置开始，如果我能知道上一次每个partition中的数据都取到什么位置了，这就得手动设置，所以说最直接有效的办法就是将offset情况回写到zookeeper中
      * 如果将checkpoint存储在HDFS上，每隔几秒都会向HDFS上进行一次写入操作而且大部分都是小文件，且不说写入性能怎么样，就小文件过多，对整个Hadoop集群都不太友好。因为只记录偏移量信息，
      * 所以数据量非常小，zk作为一个分布式高可靠的的内存文件系统，非常适合这种场景。
      * 异常原因：
      * 因为checkpoint第一次持久化的时候会把整个相关的jar给序列化成一个二进制文件，每次重启都会从里面恢复，
      * 但是当你新的 程序打包之后序列化加载的仍然是旧的序列化文件，这就会导致报错或者依旧执行旧代码。
      * 既然如此可否，直接把上次的checkpoint删除了，不就能启动了吗？
      * 确实是能启动，但是一旦删除了旧的checkpoint，新启动的程序，只能从kafka的smallest或者largest的偏移量消费，
      * 默认是从最新的，如果是最新的，而不是上一次程序停止的那个偏移量 就会导致有数据丢失，如果是老的，那么就会导致数据重复
      */
    //ssc.checkpoint("checkpoint")


    //4、配置kafka相关参数
    //fetch.message.max.bytes", 1024 设置一次fetch 请求取得的数据最大值为1KB,默认是5MB
    /**自动提交策略
            enable.auto.commit为true，表示在auto.commit.interval.ms时间后会自动提交topic的offset，其中auto.commit.interval.ms默认值为5000ms；
            由消费者协调器（ConsumerCoordinator）每隔${auto.commit.interval.ms}毫秒执行一次偏移量的提交。手动提交需要由客户端自己控制偏移量的提交。
            "enable.auto.commit", true  "auto.commit.interval.ms", 1000
      */
    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092", "group.id" -> "Kafka_Direct","auto.offset.reset" -> "largest","enable.auto.commit"->"false") //smallest largest latest

    /**手动提交
            在有些场景我们可能对消费偏移量有更精确的管理，以保证消息不被重复消费以及消息不被丢失。假设我们对拉取到的消息需要进行写入数据库处理，
            或者用于其他网络访问请求等等复杂的业务处理，在这种场景下，所有的业务处理完成后才认为消息被成功消费，这种场景下，我们必须手动控制偏移量的提交。
            Kafka 提供了异步提交（commitAsync）及同步提交（commitSync）两种手动提交的方式。两者的主要区别在于同步模式下提交失败时一直尝试提交，
            直到遇到无法重试的情况下才会结束，同时，同步方式下消费者线程在拉取消息时会被阻塞，
            直到偏移量提交操作成功或者在提交过程中发生错误。而异步方式下消费者线程不会被阻塞，可能在提交偏移量操作的结果还未返
            回时就开始进行下一次的拉取操作，在提交失败时也不会尝试提交。
            实现手动提交前需要在创建消费者时关闭自动提交，即设置enable.auto.commit=false。然后在业务处理成功后调用commitAsync()或commitSync()方法手动提交偏移量。
            由于同步提交会阻塞线程直到提交消费偏移量执行结果返回，而异步提交并不会等消费偏移量提交成功后再继续下一次拉取消息的操作，
            因此异步提交还提供了一个偏移量提交回调的方法commitAsync(OffsetCommitCallback callback)。
            当提交偏移量完成后会回调OffsetCommitCallback 接口的onComplete()方法，
            这样客户端根据回调结果执行不同的逻辑处理。
      *
      */



    //5、定义topic
    val topics = Set("sql-2")

    //6、通过 KafkaUtils.createDirectStream接受kafka数据，这里采用是kafka低级api偏移量不受zk管理
    val dstreams: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    //7、获取kafka中topic中的数据
    val topicData: DStream[String] = dstreams.map(_._2)
    println("topicData")
    topicData.print()



    /**
      * foreach触发，task loss
      */
//      topicData.foreachRDD((rdd: RDD[String]) => {
//        try {
//          count = count + 1
//          println(count + "rdd action")
//          // println("输出分区数："+TaskContext.get.partitionId)
//          val dstream = rdd.mapPartitions(k => parseSql(k))
//          dstream.foreach(x => println())
//          println("rdd end")
//        } catch {
//          case e: Exception => println("exception" + e)
//        }
//
//      })



    /**
      *foreachPartition action
      */
    topicData.foreachRDD((rdd: RDD[String]) => {
      try {
        count = count + 1
        println(count + "rdd-action")
        // println("输出分区数："+TaskContext.get.partitionId)
        rdd.foreachPartition(iter=>
        iter.foreach(record=> parseSql(iter) )
        )
        println("rdd-end")
      } catch {
        case e: Exception => println("exception" + e)
      }

    })


    /**
      * 经过一波技术调研，最终决定选择将offset回写到zookeeper里
      * test offset
      * 1.手动将offset存储到zookeeper中，然后再从zookeeper中读取offset值
      * 2.checkpoint
       Storing Offsets
       Kafka delivery semantics in the case of failure depend on how and when offsets are stored.
       Spark output operations are at-least-once. So if you want the equivalent of exactly-once semantics,
       you must either store offsets after an idempotent output, or store offsets in an atomic transaction alongside output.
       With this integration, you have 3 options, in order of increasing reliability (and code complexity), for how to store offsets.

      * Checkpoints
        If you enable Spark checkpointing, offsets will be stored in the checkpoint.
        This is easy to enable, but there are drawbacks. Your output operation must be idempotent,
        since you will get repeated outputs; transactions are not an option. Furthermore,
        you cannot recover from a checkpoint if your application code has changed.
        For planned upgrades, you can mitigate this by running the new code at the same time as the old code
        (since outputs need to be idempotent anyway, they should not clash). But for unplanned failures that require code changes,
        you will lose data unless you have another way to identify known good starting offsets.

      * Kafka itself
        Kafka has an offset commit API that stores offsets in a special Kafka topic. By default,
        the new consumer will periodically auto-commit offsets. This is almost certainly not what you want,
        because messages successfully polled by the consumer may not yet have resulted in a Spark output operation,
        resulting in undefined semantics. This is why the stream example above sets “enable.auto.commit” to false.
        However, you can commit offsets to Kafka after you know your output has been stored, using the commitAsync API.
        The benefit as compared to checkpoints is that Kafka is a durable store regardless of changes to your application code.
        However, Kafka is not transactional, so your outputs must still be idempotent.
      */







    /**
      * 正常触发，无任务lost现象
      */
      //8、切分每一行,每个单词计为1
//    val wordAndOne: DStream[(String, Int)] = topicData.flatMap(_.split(" ")).map((_, 1))
//    //9、相同单词出现的次数累加
//    val result: DStream[(String, Int)] = wordAndOne.reduceByKey(_ + _)
//    //10、打印输出
//    result.print()


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
        // Thread.sleep(200)
        println("sql:"+ num+ "->"+sql)
      }
    }catch {
      case e: Exception => {
        println("parseSql"+e)
      }
    }
    //Thread.sleep(12000)
    println("end")
    res
  }




  def parseSqlString(iterator: Iterator[String]): String ={
    var num=0
    var res: String = null
    val writer = new PrintWriter(new File("/usr/local/kafkaData.txt"))
    try {
      while (iterator.hasNext) {
        num=num+1
        val sql = iterator.next()
        val word = sql.split("")
        // Thread.sleep(200)
        writer.println("sql:"+ num+ "->"+sql)
        println("sql:"+ num+ "->"+sql)
      }
      writer.close()
    }catch {
      case e: Exception => {
        println("parseSql"+e)
      }
    }
    //Thread.sleep(12000)
    println("end")
    res
  }





}
