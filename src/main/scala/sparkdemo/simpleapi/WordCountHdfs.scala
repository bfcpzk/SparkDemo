package sparkdemo.simpleapi

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhaokangpan on 2016/11/29.
  */
object WordCountHdfs {
  def main(args : Array[String]): Unit ={
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("WordCount")
    val sc = new SparkContext(conf)

    val path = args(0)

    val result = sc.textFile(path).flatMap(l => {
      val p = l.split(" ")
      p
    }).map(l => (l, 1)).reduceByKey(_+_).collect().foreach(l => println(l))
  }

}
