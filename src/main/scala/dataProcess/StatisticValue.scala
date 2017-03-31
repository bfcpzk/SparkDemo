package dataProcess

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by zhaokangpan on 2017/3/31.
  */
object StatisticValue {

  case class DataObj(fa : Double, fb : Double, label : Double)

  def main(args : Array[String]): Unit ={
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //配置文件
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    //数据读取
    val file = sc.textFile("testSet.txt").map( l => {
      val p = l.split("\t")
      DataObj(p(0).toDouble, p(1).toDouble, p(2).toDouble)
    }).cache()//封装成对象

    //基本属性计算
    println("fa : " + file.map(l => l.fa).stats())
    println("fb : " + file.map(l => l.fb).stats())
    println("label : " + file.map(l => l.label).stats())
  }
}
