import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.util.MLUtils


object DTUL {

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val conf = new SparkConf().setAppName("DTUL")
    val sc = new SparkContext(conf)

    // $example on$
    // Load and parse the data file.
    val trainSet = args(0)
    val depth = args(1).toInt
    val bins = args(2).toInt
    val im = args(3)

    val data = MLUtils.loadLibSVMFile(sc,trainSet).repartition(100)

    // Split the data into training and test sets (30% held out for testing)
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a DecisionTree model.
    //  Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses = 8477
    val categoricalFeaturesInfo = Range(0,502723,1).map(f => (f,2)).toMap
    val impurity = im
    val maxDepth = depth
    val maxBins = bins

    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)

    // Evaluate model on test instances and compute test error
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / testData.count()
    println("Test Error = " + testErr)
    println("Learned classification tree model:\n" + model.toDebugString)

    // Save and load model
//    model.save(sc, "target/tmp/myDecisionTreeClassificationModel")
//    val sameModel = DecisionTreeModel.load(sc, "target/tmp/myDecisionTreeClassificationModel")
    // $example off$
  }
}
// scalastyle:on println
