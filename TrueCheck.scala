import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object TrueCheck {
  def main(args: Array[String]): Unit ={
    Logger.getRootLogger.setLevel(Level.WARN)
    val conf = new SparkConf().setAppName("ED")
    val sc = new SparkContext(conf)

    val ub = args(0)
    val test = args(1)
    val output = args(2)
//    val k = args(3).toInt
    val trainSet = sc.textFile(ub).flatMap{
      line =>
        val shop = line.split(",")(1)
        line.split(",")(5).split(";").map{
          s => if(s.split("\\|")(2).equals("true"))
            (shop,s.split("\\|")(0))
          else (shop,"liujie")
        }
    }.filter(! _._2.equals("liujie"))
      .map{
        case(shop,wifi) => (shop,List(wifi))
      }.reduceByKey(_:::_).map{
      case(shop,wifis) =>(shop,wifis.toSet)
    }.collect()

    sc.broadcast(trainSet)

    val testSet = sc.textFile(test).flatMap{
      line =>
        val rowid = line.split(",")(0)
        line.split(",")(6).split(";").map{
          s => if(s.split("\\|")(2).equals("true"))
            (rowid,s.split("\\|")(0))
          else (rowid,"liujie")
        }
    }.filter(! _._2.equals("liujie"))
      .map{
        case(rowid,wifi) => (rowid,List(wifi))
      }.reduceByKey(_:::_).map{
      case(rowid,wifis) =>(rowid,wifis.toSet)
    }


    val res = testSet.flatMap{
      case (rowid,wifis1) =>
        trainSet.map{
          case(shop, wifis2) =>
            val distance = computeDistance(wifis1,wifis2)
            (rowid,List((shop,distance)))
        }
    }.reduceByKey(_:::_).map{
      case(rowid,list) =>
        val head = list.sortBy(_._2).head
        if(head._2 == 0)
          rowid+","+"s_liujie"
        else
          rowid+","+head._1
    }
    val testtrue = testSet.map{
      f => f._1
    }.collect().toSet

    sc.broadcast(testtrue)
    val testfalse = sc.textFile(test).map{
      f => f.split(",")(0)
    }.filter(! testtrue.contains(_))
      .map{
        f => f+",s_liujie"
      }
    val res2 = res.union(testfalse).sortBy(_.split(",")(0))
    check(output,sc)
    res2.saveAsTextFile(output)
    sc.stop()
  }
  def computeDistance(a: Set[String], b: Set[String]): Int ={
    a.&(b).size
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
