import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * Created by ad on 2017/2/17.
  */
object shareTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Test").setMaster("local[4]")
    System.setProperty("hadoop.home.dir", "D:\\Java\\hadoop-2.6.5");
    val context = new SparkContext(conf)

    var shareCount = 0
    val rdd = context.parallelize(List(1,2,3,4,5,6,7,8))

   // val rdd2 = rdd.map(data => {shareCount += data;data+10})
    val accum = context.accumulator(0,"accum")
    rdd.foreach(x => accum += x)
    println(accum.value)

  }
}

object broadcast{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Test").setMaster("local[4]")
    System.setProperty("hadoop.home.dir", "D:\\Java\\hadoop-2.6.5");
    val context = new SparkContext(conf)

    val rdd1 = context.parallelize(Array(1,2,3,4,5))
    val broadcastValue = context.broadcast(10)
    val rdd2 = rdd1.map(x => x*broadcastValue.value)
    rdd2.collect()
    println(rdd2.collect().toBuffer)
  }
}

object sqlSpark{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Test").setMaster("local[4]")
    System.setProperty("hadoop.home.dir", "D:\\Java\\hadoop-2.6.5");
    val context = new SparkContext(conf)
    val sqlContext = new SQLContext(context)
    //val rdd = context.textFile("D:\\data.txt").map(_.split(" ")).map(x => person(x(0),x(1),x(2)))
    val schema = "id name age"
    val data = StructType(schema.split(" ").map(fieldName => StructField(fieldName,StringType,true)))
    val rowRDD = context.textFile("D:\\data.txt").map(_.split(" ")).map(p => Row(p(0),p(1),p(2)))
    val schemaRDD = sqlContext.applySchema(rowRDD,data)
    schemaRDD.registerTempTable("People")

    val result = sqlContext.sql("select name from People")
    result.map(t => "name: " + t(0)).collect().foreach(println)
  }
}

object sqlSpark2{

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Test").setMaster("local[4]")
    System.setProperty("hadoop.home.dir", "D:\\Java\\hadoop-2.6.5");
    val context = new SparkContext(conf)
    val sqlContext = new SQLContext(context)


    val people = context.textFile("D:\\data.txt").map(_.split(" ")).map(p => Person(p(0).toInt,p(1),p(2).toInt))
    println(people.collect().toBuffer)
    import sqlContext.implicits._
    val rddDF = people.toDF()

    rddDF.registerTempTable("People")
    sqlContext.sql("select * from People").map(x => "id: "+x(0)+" name: "+x(1)+" age: "+x(2)).collect().foreach(println)
    context.stop()
  }



}

object SparkStream{

  def updateFunc = ???

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Test").setMaster("local[4]")
    System.setProperty("hadoop.home.dir", "D:\\Java\\hadoop-2.6.5");
    val context = new SparkContext(conf)
    val streamContext = new StreamingContext(context,Seconds(5))
    val ssc = streamContext.socketTextStream("192.168.186.101",8888)

      streamContext.checkpoint("hdfs://192.168.186.101:9000/spark")
   val wordCount = ssc.flatMap(_.split(" ")).map((_,1)).updateStateByKey(updateFunc,new HashPartitioner(
     streamContext.sparkContext.defaultParallelism),true)

    wordCount.print()
    streamContext.start()
    streamContext.awaitTermination()
  }
}


case class Person(id: Int, name: String, age: Int)