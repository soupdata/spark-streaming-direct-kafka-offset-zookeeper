package mllib
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.util.MLUtils
object PsbysAs {
  def main(args:Array[String]):Unit={

    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("NaiveBayes")
    val sc = new SparkContext(conf)
    // Load and parse the data file.
    // "/usr/local/spark/data/mllib/sample_libsvm_data.txt"
    val data = MLUtils.loadLibSVMFile(sc, "/usr/local/data/train.txt")

    println(data)

    // Split data into training (60%) and test (40%).
    val Array(training, test) = data.randomSplit(Array(0.6, 0.4))

    val model = NaiveBayes.train(training, lambda = 1.0, modelType = "multinomial")

    println(model)

    val predictionAndLabel = data.map(p => (model.predict(p.features), p.label))
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()

    // Save and load model
    model.save(sc, "target/tmp/myNaiveBayesModel")
    val sameModel = NaiveBayesModel.load(sc, "target/tmp/myNaiveBayesModel")

  }
}
