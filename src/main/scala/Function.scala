import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ad on 2017/2/16.
  */
object Function {
  val func = (index:Int,iter:Iterator[(Int)]) => {
    iter.toList.map(x => "PartId = " + index + ",val = "+ x).iterator
  }
  val conf = new SparkConf()
  val sc = new SparkContext()
  val rdd1 = sc.parallelize(List(1,2,3,4,5,6,7,8),2)
  rdd1.mapPartitionsWithIndex(func).collect()
  rdd1.aggregate(0)(math.max(_,_),_+_)
}
