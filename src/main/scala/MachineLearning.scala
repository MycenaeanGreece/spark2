import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint,LinearRegressionWithSGD}
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.NaiveBayes


/**
  * Created by ad on 2017/2/19.
  */
object MachineLearning {
  def main(args: Array[String]): Unit = {
    val inputPath = "D:\\文档\\学习\\寒假实训\\SparkML\\spark_mlib-day01\\bayes_football.txt.txt"
    val conf = new SparkConf().setAppName("ML").setMaster("local")
    System.setProperty("hadoop.home.dir", "D:\\Java\\hadoop-2.6.5");
    val sc = new SparkContext(conf)

    val data = sc.textFile(inputPath)

    val parserData = data.map{
      line => val splits = line.split(",")
        LabeledPoint(splits(0).toDouble,Vectors.dense(splits(1).split(" ").map(_.toDouble)))
    }

    val parts = parserData.randomSplit(Array(0.6,0.4),seed = 11L)
    val trainingData = parts(0)
    val testData = parts(1)


    val model = NaiveBayes.train(trainingData,lambda = 1.1, modelType = "multinomial")

    val predictionAndLabel = testData.map(v => (model.predict(v.features),v.label))
    val accuracy = 1.0 * predictionAndLabel.filter(r => r._1 == r._2).count() / (testData.count())
    println("accuracy="+accuracy)

    println("prediction of (0.0, 2.0, 0.0, 1.0):)"+model.predict(Vectors.dense(0.0, 2.0, 0.0, 1.0)))

  }

}

//object DecisiongTreeDemo{
//  def main(args: Array[String]): Unit = {
//    val inputPath = "D:\\文档\\学习\\寒假实训\\SparkML\\spark_mlib-day01\\covtype_decision_tree.data"
//    val conf = new SparkConf().setAppName("ML").setMaster("local(4)")
//    System.setProperty("hadoop.home.dir", "D:\\Java\\hadoop-2.6.5");
//    val sc = new SparkContext(conf)
//
//    val data = sc.textFile(inputPath).map{
//      line => val values = line.split(",").map(_.toDouble)
//        val featureFactor = Vectors.dense(values.init)
//        val label = values.last-1
//        LabeledPoint(label,featureFactor)
//    }
//
//    val Array(trainData,cvData,testData) = data.randomSplit(Array(0.8,0.1,0.1))
//    trainData.cache()
//    cvData.cache()
//    testData.cache()
//
//    val model = DecisionTree.trainClassifier(trainData,7,Map[Int,Int](),"gini",4,100)
//    val metrics = getMetrics(model,cvData)
//
//    println("精确度："+metrics.precision)
//
//    println("召回率:"+metrics.recall)
//
//    (0 until 7).map(
//      cat => (metrics.precision(cat),metrics.recall(cat))
//    ).foreach(println)
//    }
//
//  val evaluation =
//    for (impurity <- Array("gini","entropy");
//         depth    <- Array(1,20);
//         bins     <- Array(10,300))
//      yield{
//        val model = DecisionTree.trainClassifier(trainData,7,Map[Int,Int](),impurity,depth,bins)
//        val predictionAndLabels = cvData.map(example =>
//          (model.predict(example.features),example.label)
//        )
//        val accuracy = new MulticlassMetrics(predictionAndLabels).precision
//        ((impurity,depth,bins),accuracy)
//      }
//  evaluation.sortBy(_._2).reverse.foreach(println)
//
//}
//
//  def getMetrics(model:DecisionTreeModel,data:RDD[LabeledPoint]):
//  MulticlassMetrics ={
//    val pridictionAndLabels = data.map{
//      example => (model.predict(example.features),example.label)
//    }
//
//    new MulticlassMetrics(pridictionAndLabels)
//  }
//}

object DecisionTreeDemo {

  def main(args: Array[String]): Unit = {
    val inputpath = "D:\\文档\\学习\\寒假实训\\SparkML\\spark_mlib-day01\\covtype_decision_tree.data"
    System.setProperty("hadoop.home.dir", "D:\\Java\\hadoop-2.6.5");
    val conf = new SparkConf().setMaster("local").setAppName("DecisionTree")
    val sc = new SparkContext(conf)

    //处理预处理，把每行字符串转换成LabelPoint(特征向量)
    val data = sc.textFile(inputpath).map{line =>
      val values = line.split(',').map(_.toDouble)
      val featureVector = Vectors.dense(values.init)
      val label = values.last - 1
      LabeledPoint(label,featureVector)
    }

    //设定训练集、交叉集、验证集
    val Array(trainData,cvData,testData) = data.randomSplit(Array(0.8,0.1,0.1))
    trainData.cache()
    cvData.cache()
    testData.cache()

    //构建决策树模型，最大深度，纯度度量方法，最大桶数
    //桶：决策规则集
    val model = DecisionTree.trainClassifier(trainData,7,Map[Int,Int](),"gini",4,100)

    //在CV集上计算决策树模型的性能指标
    val metrics = getMetrics(model,cvData)

    //打印性能指标：精确度
    println("精确度："+metrics.precision)

    //打印召回率
    println("召回率:"+metrics.recall)

    //打印每个类别的准确度和召回率
    (0 until 7).map(
      cat => (metrics.precision(cat),metrics.recall(cat))
    ).foreach(println)

    val evaluation =
      for (impurity <- Array("gini","entropy");
           depth    <- Array(1,20);
           bins     <- Array(10,300))
        yield{
          val model = DecisionTree.trainClassifier(trainData,7,Map[Int,Int](),impurity,depth,bins)
          val predictionAndLabels = cvData.map(example =>
            (model.predict(example.features),example.label)
          )
          val accuracy = new MulticlassMetrics(predictionAndLabels).precision
          ((impurity,depth,bins),accuracy)
        }

    evaluation.sortBy(_._2).reverse.foreach(println)

  }

  /**
    * 获取一个决策树性能指标评估器（Metrics），MulticlassMetrics：适用于多分类
    * @param model
    * @param data
    * @return
    */
  def getMetrics(model: DecisionTreeModel, data: RDD[LabeledPoint]): MulticlassMetrics  ={
    val pridictionAndLabels = data.map{example =>
      (model.predict(example.features),example.label)
    }
    new MulticlassMetrics(pridictionAndLabels)
  }
}

object LinearRegressionDemo{
  def main(args: Array[String]): Unit = {
    val inputpath = "D:\\文档\\学习\\寒假实训\\SparkML\\spark_mlib-day01\\linear_regression.data"
    System.setProperty("hadoop.home.dir", "D:\\Java\\hadoop-2.6.5");
    val conf = new SparkConf().setMaster("local").setAppName("DecisionTree")
    val sc = new SparkContext(conf)

    val parsedData = sc.textFile(inputpath).map{
      line => val values = line.split(',')
        LabeledPoint(values(0).toDouble,Vectors.dense(values(1).split(' ').map(_.toDouble)))
    }

    val Array(trainData,testData) = parsedData.randomSplit(Array(0.8,0.2))
    trainData.cache()
    testData.cache()

    val numIterations = 100
    val stepSize = 0.00000001
    val model = LinearRegressionWithSGD.train(trainData,numIterations,stepSize)

    val valueAndPreds = testData.map{point =>
      val prediction = model.predict(point.features)
      (point.label,prediction)
    }

    val MSE = valueAndPreds.map{ case(v,p) => math.pow((v-p),2)}.mean()
    println("training Mean Squared Error: " + MSE)

  }
}

