import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object CD {
  def main(args: Array[String]): Unit ={
    Logger.getRootLogger.setLevel(Level.WARN)
    val conf = new SparkConf().setAppName("ED")
    val sc = new SparkContext(conf)

    val ub = args(0)
    val test = args(1)
    val output = args(2)

    val shopwifiss = sc.textFile(ub).flatMap{
      line =>
        val shop = line.split(",")(1)
        val f = line.split(",")(5).split(";")
        f.map{
          s => ((shop,s.split("\\|",-1)(0)),1)
        }
    }.reduceByKey(_+_).map{
      f => (f._1._1,List((f._1._2,f._2)))
    }.reduceByKey(_:::_).map{
      case(shop, wifis) => (shop, wifis)
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
      case(a,v1) =>
        shopwifiss.map {
          case (b, v2) =>
            val similarity = computeSimi(v1,v2)
            (a,(b,similarity))
        }
    }.reduceByKey((a,b) => if(a._2>b._2) a else b).distinct().map{
      case(rowid,predict) => rowid+","+predict._1
    }
    println(res.count())
    check(output,sc)
    res.saveAsTextFile(output)
  }
  def computeSimi(a: List[String], b: List[(String,Int)]): Int ={
        val map = b.toMap
        var count = 0
        a.foreach(f => if(map.contains(f)) count = count + map.getOrElse(f, 0))
        count
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
