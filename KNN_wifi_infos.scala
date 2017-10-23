import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object KNN_wifi_infos {
  def main(args: Array[String]): Unit ={
    Logger.getRootLogger.setLevel(Level.WARN)
    val conf = new SparkConf().setAppName("KNN")
    val sc = new SparkContext(conf)

    val ub = args(0)
    val test = args(1)
    val output = args(2)
    val k = args(3).toInt
    val trainSet = sc.textFile(ub).map{
      line =>
        val shop = line.split(",")(1)
        val wifis = line.split(",")(5).split(";").map{
          s => s.split("\\|",-1)(0)
        }
        (shop, List(wifis.toSet))
    }.reduceByKey(_:::_).cache()

    val testSet = sc.textFile(test).map {
      line =>
        val rowid = line.split(",")(0)
        val wifis = line.split(",")(6).split(";").map {
          s => s.split("\\|", -1)(0)
        }
        (rowid, List(wifis.toSet))
    }.reduceByKey(_:::_).map{
      case(row,list) => (row, list.head)
    }.cache()

    val res = trainSet.cartesian(testSet).flatMap{
      case ((shop,wifis),(rowid, wifi)) =>
        computeDistance(wifis,wifi).map{
          f => (rowid,List((shop,f)))
        }
    }.reduceByKey(_:::_).map{
      case(rowid,list) =>
        val kneighbors = list.sortBy(_._2).take(k).map(f => (f._1,1))
        val predict = kneighbors.groupBy(_._1).map{
          case(shop,l) => (shop,l.length)
        }.toArray.maxBy(_._2)
        rowid+","+predict._1
    }
    check(output,sc)
    res.saveAsTextFile(output)
    sc.stop()
  }
  def computeDistance(a: List[Set[String]], b: Set[String]): List[Int] ={
    a.map{
      f =>  f.--(b).size + b.--(f).size
    }

  }
  private def check(fileName: String,sc: SparkContext): Unit = {
    import org.apache.hadoop.fs.{FileSystem, Path}
    import java.net.URI
    val fs = FileSystem.get(URI.create(fileName),sc.hadoopConfiguration)
    val isExists = fs.exists(new Path(fileName))
    if (isExists) {
      val isDel = fs.delete(new Path(fileName), true)
      println(fileName + "  delete?\t" + isDel)
    } else {
      println(fileName + "  exist?\t" + isExists)
    }
  }
}
