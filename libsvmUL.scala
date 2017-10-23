import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lenovo on 2017/10/23.
  */
object libsvmUL {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val conf = new SparkConf().setAppName("libsvmUL")
    val sc = new SparkContext(conf)

    val ub = args(0)
    val test = args(1)
    val traino = args(2)
    val testo = args(3)

    val shops = sc.textFile(ub).map{
      f => f.split(",")(1)
    }.distinct().zipWithUniqueId().map(f => (f._1,f._2+1)).collectAsMap()

    sc.broadcast(shops)
    val trainWifis = sc.textFile(ub).flatMap {
      line =>
        line.split(",")(5).split(";").map {
          s => s.split("\\|", -1)(0)
        }
    }
    val testWifis = sc.textFile(test).flatMap {
      line =>
        line.split(",")(6).split(";").map {
          s => s.split("\\|", -1)(0)
        }
    }
    val map = trainWifis.union(testWifis).distinct().zipWithUniqueId().map(f => (f._1,f._2+1)).collectAsMap()
    println(trainWifis.union(testWifis).distinct().count())
    sc.broadcast(map)

    val testSet = sc.textFile(test).map {
      line =>
        val rowid = line.split(",")(0)
        val wifis = line.split(",")(6).split(";").map {
          s => map.getOrElse(s.split("\\|", -1)(0),-1.0.toLong)
        }.filter(f => f != -1.0).distinct.sortBy(f => f).map(_ + ":1.0")
        rowid+ " "+wifis.mkString(" ")
    }

    val trainSet = sc.textFile(ub).map{
      line =>
        val shopid = shops.getOrElse(line.split(",")(1), -1.toLong)
        val wifis = line.split(",")(5).split(";").map{
          s => map.getOrElse(s.split("\\|",-1)(0), -1.0.toLong)
        }.filter(f => f != -1.0).distinct.sortBy(f =>f ).map(_+ ":1.0")
        (shopid,wifis.mkString(" "))
    }.filter(_._1 != -1).map(f =>f._1+" "+f._2)

    check(testo,sc)
    check(traino,sc)
    testSet.saveAsTextFile(testo)
    trainSet.saveAsTextFile(traino)

    sc.stop()
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
