import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 直接广播一个变量，然后flatMap，最终reduceByKey取head  这个方案最快
  */
object MAP_UL {
  def main(args: Array[String]) {
    Logger.getRootLogger.setLevel(Level.WARN)
    val conf = new SparkConf().setAppName("UL_MAP")
    val sc = new SparkContext(conf)

    val user_behavior_path = args(0)
    val test_path = args(1)
    val output = args(2)

    val shop_wifis = sc.textFile(user_behavior_path).flatMap {
      line =>
        val shop = line.split(",")(1)
        val wifis = line.split(",")(5).split(";")
        wifis.map {
          wifi =>
            val f = wifi.split("\\|")
            if(f.length < 2) println(wifi)
            ((shop, f(0)), (f(1).toDouble,1))
        }
    }.reduceByKey( (a,b) => (a._1+b._1,a._2+b._2))
      .map(f => ((f._1._1), List((f._1._2, f._2._1/f._2._2))))
      .reduceByKey(_ ::: _)
      .map {
        case (shop, wifiList) =>
          val arr = wifiList.sortBy(_._2).map(f => f._1).toArray
          (shop, arr)
      }.collect()

    sc.broadcast(shop_wifis)

    val test = sc.textFile(test_path).flatMap {
      line =>
        val rowid = line.split(",")(0)
        val wifis = line.split(",")(6).split(";")
        wifis.map {
          wifi =>
            val f = wifi.split("\\|")
            ((rowid, f(0)), f(1).toDouble)
        }
    }.reduceByKey(_ + _).map(f => ((f._1._1), List((f._1._2, f._2))))
      .reduceByKey(_ ::: _)
      .map {
        case (rowid, wifiList) =>
          val arr = wifiList.sortBy(_._2).map(f => f._1).toArray
          (rowid, arr)
      }.cache()

    val row_shop = test.flatMap{
      case(rowid, arr) =>
        shop_wifis.map{
          case(shop, wifis) =>
            val score = computeScore(wifis, arr)
            (rowid, (shop,score))
        }
    }.reduceByKey((a, b) => if(b._2>a._2) b else a )
      .map{
        case(rowid, t) =>
          rowid+","+t._1
      }
    check(output,sc)
    row_shop.saveAsTextFile(output)

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

  private def computeScore(wifi: Array[String], arr: Array[String]): Double = {
    val map = wifi.zipWithIndex.toMap
    var i = 0
    arr.map{
      wifi => if(map.contains(wifi)) i = i + 1
        i/(map.getOrElse(wifi,Int.MaxValue-1)+1)
    }.sum
  }
}
