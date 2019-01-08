package mllib
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

object test {
  def main(args:Array[String]):Unit={
      val conf = new SparkConf()   //创建环境变量
        .setMaster("local")        //设置本地化处理
        .setAppName("stock_AS") //设定名称
      val sc = new SparkContext(conf)
      sc.setLogLevel("ERROR")
      val data = sc.textFile("/usr/local/data/train.txt")
      val parsedData = data.map { line =>
        val parts = line.split(',')
        LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
      }

      println("获取训练和测试的数据")
      val splits = parsedData.randomSplit(Array(0.7, 0.3), seed = 11L)			//对数据进行分配
      val trainingData = splits(0)									//设置训练数据
      val testData = splits(1)
      println("开始训练了")//设置测试数据
      val model = NaiveBayes.train(trainingData, lambda = 1.0)			//训练贝叶斯模型
      println("训练结束了")
      val predictionAndLabel = testData.map(p => (model.predict(p.features), p.label)) //验证模型
      val accuracy = 1.0 * predictionAndLabel.filter(					//计算准确度
        label => label._1 == label._2).count()						//比较结果
      // println(accuracy)
      val test = Vectors.dense(0, 0, 1,4,1,0)
      val result = model.predict(test)//预测一个特征　
      println(result)//2
      model.save(sc, "target/tmp/myNaiveBayesModel")
      val sameModel = NaiveBayesModel.load(sc, "target/tmp/myNaiveBayesModel")
  }

}
