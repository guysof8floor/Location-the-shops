import org.apache.spark.mllib.linalg.{Matrix, SingularValueDecomposition}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.util.MLUtils

/**
  * Created by lenovo on 2017/10/23.
  */
object SVDUL {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("PCAUL")
    val sc = new SparkContext(conf)
    val trainSet = args(0)
    val k = args(1).toInt
    val output = args(2)

    val data = MLUtils.loadLibSVMFile(sc, trainSet)
    val features = data.map(_.features).repartition(100).cache()
    val label  = data.map(_.label)
    val mat: RowMatrix = new RowMatrix(features)

    val svd: SingularValueDecomposition[RowMatrix, Matrix] = mat.computeSVD(k, computeU = false)
    val V: Matrix = svd.V // The V factor is a local dense matrix.

    val res = label.zip(mat.multiply(V).rows).map{
      case (l,f) => LabeledPoint(l,f)
    }
    MLUtils.saveAsLibSVMFile(res,output)

    sc.stop()
  }
}
