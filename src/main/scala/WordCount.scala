/**
  * Created by ad on 2017/2/16.
  */
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hadoop on 2017/2/9 0009.
  */
object WordCount {

  def main(args: Array[String]): Unit = {

    val sconf = new SparkConf().setAppName("hellospark")

    val sc = new SparkContext(sconf)

    val rdd = sc.textFile(args(0)).flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _).sortBy(_._2, false).saveAsTextFile(args(1))
    sc.stop()
    val t = new StorageLevel
  }
}

