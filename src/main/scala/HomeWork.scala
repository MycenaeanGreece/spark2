/**
  * Created by ad on 2017/2/16.
  */
//package cn.youe.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by youe on 2017/2/16.
  */
object HomeWork {
  def main(args: Array[String]): Unit = {
    //本地跑local (本次本地就是现在windows环境),[4]:在本地启动4个线程
    val conf = new SparkConf().setAppName("HomeWork").setMaster("local[4]")
    System.setProperty("hadoop.home.dir", "D:\\Java\\hadoop-2.6.5");
    val context = new SparkContext(conf)

    val rddLacTime = context.textFile("D:\\文档\\学习\\寒假实训\\scala讲课ppt\\spark01\\spark\\经纬度作业\\DDE7970F68.log")
      .map(line => {
        val fields = line.split(",")
        val telphone = fields(0)
        var time = fields(1).toLong
        val lacId = fields(2)
        val flag = fields(3).toInt

        time = if(flag==1) -time else time
        ((telphone,lacId),time)
      }).reduceByKey(_+_).map(line => {(line._1._1,(line._1._2,line._2))})

    val rddMaxTime =  rddLacTime.groupByKey().mapValues(ite => {
      ite.toList.sortBy(_._2).reverse.take(1)
    }).map(data=> {
      (data._2(0)._1 , (data._1 , data._2(0)._2))
    })

    val rddLacStation = context.textFile("D:\\文档\\学习\\寒假实训\\scala讲课ppt\\spark01\\spark\\经纬度作业\\loc_info.txt")
      .map(line =>{
        val fields = line.split(",")
        val lacId = fields(0)
        (lacId ,(fields(1),fields(2)))
      })
    rddLacStation.partitions
    val rddJoin = rddMaxTime.join(rddLacStation)
    val rddResult = rddJoin.map(item => {
      (item._2._1._1 , item._2._1._2,item._1,item._2._2)
    })
    print(rddResult.collect().toBuffer)


  }
}

