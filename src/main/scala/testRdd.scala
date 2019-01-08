import org.apache.spark.{SparkConf, SparkContext}

object testRdd {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("spark demo example ").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.parallelize(Seq(1, 2, 3, 4, 5), 3)

    rdd.foreachPartition(partiton => {
      // partiton.size 不能执行这个方法，否则下面的foreach方法里面会没有数据，
      //因为iterator只能被执行一次
      partiton.foreach(line => {
        //save(line)  落地数据
        println(line)
      })

    })
    sc.stop()
  }
}
