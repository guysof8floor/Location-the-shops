import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object MPUL {
  def main(args: Array[String]): Unit ={
    Logger.getRootLogger.setLevel(Level.WARN)
    val conf = new SparkConf().setAppName("MPUL")
    val sc = new SparkContext(conf)

    val ub = args(0)
    val test = args(1)
    val output = args(2)

    val shopwifiss = sc.textFile(ub).flatMap{
      line =>
        val shop = line.split(",")(1)
        val f = line.split(",")(5).split(";")
        f.map{
          s => ((shop, s.split("\\|",-1)(0)),1)
        }
    }.reduceByKey(_+_).map{
      f => (f._1._1,List((f._1._2,f._2)))
    }.reduceByKey(_:::_).map{
      case(shop, wifis) => val sum = wifis.map(f => f._2).sum
        val wifisp = wifis.map(f => (f._1,f._2/sum.toDouble))
        (shop,wifisp)
    }.collect()

    sc.broadcast(shopwifiss)

    val testwifis = sc.textFile(test).flatMap{
      line =>
        val rowid = line.split(",")(0)
        val f = line.split(",")(6).split(";")
        f.map{
          s => (rowid,s.split("\\|",-1)(0))
        }
    }.distinct().map{
      f => (f._1,List(f._2))
    }.reduceByKey(_:::_).map{
      case(rowid, wifis) => (rowid,wifis)
    }.cache()

    val res = testwifis.flatMap{
      case(rowid,wifis) =>
        shopwifiss.map {
          case (shop, wifisp) =>
            val similarity = computeSimi(wifis,wifisp)
            (rowid,(shop,similarity))
        }
    }.reduceByKey((a,b) => if(a._2>b._2) a else b).distinct().map{
      case(rowid,predict) => rowid+","+predict._1
    }
    println(res.count())

    check(output,sc)
    res.saveAsTextFile(output)
  }
  def computeSimi(a: List[String], b: List[(String,Double)]): Double ={
    val map = b.toMap
    val len = Math.min(a.length, b.length)
    var count = 0.0
    a.foreach(f => if(map.contains(f)) count = count + map.getOrElse(f, 0.0))
    count/len
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
