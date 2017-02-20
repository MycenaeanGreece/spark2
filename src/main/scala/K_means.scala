import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by ad on 2017/2/20.
  */
object K_means {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    System.setProperty("hadoop.home.dir", "D:\\Java\\hadoop-2.6.5");
    val inputpath = "D:\\文档\\学习\\寒假实训\\SparkML\\spark-mllib-02\\kmeans_data.txt"
    val conf = new SparkConf().setAppName("KMeans").setMaster("local")
    val sc = new SparkContext(conf)


    val data = sc.textFile(inputpath).map(point => Vectors.dense(point.split(" ").map(_.toDouble))).cache()

    val numClustets = 3
    val numIteration = 20
    val model01 = KMeans.train(data,numClustets,numIteration)

    val WSSSE = model01.computeCost(data)

    println(s"Within Set Sum of Squared Errors: $WSSSE")
    println("Model01 Clustet Center is ")
    model01.clusterCenters.foreach(println)

    val kMeans = new KMeans()
    val model02 = kMeans.run(data)

    println("Model02 Clustet Center is ")
    model02.clusterCenters.foreach(println)

  }
}

object netAttack{
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    System.setProperty("hadoop.home.dir", "D:\\Java\\hadoop-2.6.5");
    val inputpath = "D:\\文档\\学习\\寒假实训\\SparkML\\spark-mllib-02\\kddcup2.data_10_percent"
    val conf = new SparkConf().setAppName("KMeans").setMaster("local")
    val sc = new SparkContext(conf)

//    val Type = sc.textFile(inputpath).map(line => line.split(",").last).map((_,1)).reduceByKey(_+_).cache()
//    Type.foreach(println)
//    val data2 = new StringBuffer()
    val data = sc.textFile(inputpath).map(point => Vectors.dense(point.split(" ").map(_.toDouble))).cache()
//    val datasource = sc.textFile(inputpath).map{line =>
//      val buffer = line.split(",").toBuffer
//      buffer.remove(1,3)
//      buffer.remove(buffer.length-1)
//      val feature = Vectors.dense(buffer.map(_.toDouble).toArray)
//      feature
//    }

//    val data = sc.textFile(inputpath).map(line => Vectors.dense(line.split(" ").map(_.toDouble))).cache()


//    val model01 = KMeans.train(data,22,20)
//    val WSSSE = model01.computeCost(data)

//    println(s"Within Set Sum of Squared Errors: $WSSSE")
//    println("Model01 Clustet Center is ")
//    model01.clusterCenters.foreach(println)
    for(i <- 1 to 30){
      println(i,test(i,data))
    }


  }

  def test(k:Int,data:RDD[Vector]): Double = {
    val model = KMeans.train(data,k,20)
    val WSSSE = model.computeCost(data)
    WSSSE
  }

}

//val data = sc.textFile("D:\\Program Files (x86)\\QQfile\\spark-mllib-021\\spark-mllib-02\\kddcup.data_10_percent")
//
//data.map(line => line.split(',').last).countByValue().toSeq.sortBy(_._2).reverse.foreach(println)
//
//val labelsAndData = data.map{line =>
//  val buffer = line.split(',').toBuffer
//  buffer.remove(1,3)
//  val label = buffer.remove(buffer.length-1)
//  val feature = Vectors.dense(buffer.map(_.toDouble).toArray)
//  feature
//}

