package sparkdemo.simpleapi

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by zhaokangpan on 2016/11/29.
  */
object WordCount {
  def main(args : Array[String]): Unit ={
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val path = "dreamore.txt"

    val result = sc.textFile(path).flatMap(l => {
      val p = l.split("\t")
      for(term <- p(1).split(" ")) yield (term, 1)
    }).reduceByKey(_+_).collect().foreach(l => println(l))
  }

}
