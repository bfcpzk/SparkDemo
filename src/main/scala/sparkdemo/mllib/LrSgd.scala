package sparkdemo.mllib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LinearRegressionWithSGD, LabeledPoint}
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer
/**
  * Created by zhaokangpan on 16/7/11.
  */
object LrSgd {
  def main(args : Array[String]): Unit ={
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //设置运行环境
    val conf = new SparkConf().setAppName("LrSgd").setMaster("local[4]")
    val sc = new SparkContext(conf)

    //读取数据,稀疏存储
    val data = sc.textFile("testSet.txt").map(line => {
      val parts = line.split("\t")
      val index = ArrayBuffer[Int]()
      val value = ArrayBuffer[Double]()
      for( i <- 0 until parts.length - 1 ){
        if(parts(i).toDouble != 0.0){
          index.+=(i)
          value.+=(parts(i).toDouble)
        }
      }
      LabeledPoint(parts(2).toDouble, Vectors.sparse(2, index.toArray, value.toArray))
    }).cache()

    val splitData = data.randomSplit(Array(0.7, 0.3), seed = 11L)

    val train_data = splitData(0).cache()
    val test_data = splitData(1)

    //设置迭代次数,以及修正步长
    val numIterations = 800 //迭代次数
    val stepSize = 0.01 //步长
    val model = LinearRegressionWithSGD.train(train_data, numIterations, stepSize)

    // 在测试集上计算预测结果.
    val scoreAndLabels = test_data.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    // 获得评分矩阵.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC

    //将训练好的权重用,分割
    val resultString = model.weights.toArray.mkString(",")

    println(resultString)
    println(auROC)
  }
}