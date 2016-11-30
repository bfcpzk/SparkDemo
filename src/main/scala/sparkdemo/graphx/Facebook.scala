package sparkdemo.graphx

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by zhaokangpan on 16/10/25.
  */
object Facebook {

  def mergeMaps(m1 : Map[VertexId, Int], m2 : Map[VertexId, Int]) : Map[VertexId, Int] = {
    def minThatExists(k : VertexId) : Int = {
      math.min(m1.getOrElse(k, Int.MaxValue), m2.getOrElse(k, Int.MaxValue))
    }

    (m1.keySet ++ m2.keySet).map{
      k => (k, minThatExists(k))
    }.toMap
  }

  def update(id : VertexId, state : Map[VertexId, Int], msg : Map[VertexId, Int]) = {
    mergeMaps(state, msg)
  }

  def checkIncrement(a : Map[VertexId, Int], b : Map[VertexId, Int], bid : VertexId) = {
    val aplus = a.map{ case(v,d) => v -> (d+1)}
    if(b != mergeMaps(aplus, b)){
      Iterator((bid, aplus))
    }else{
      Iterator.empty
    }
  }

  def iterate(e : EdgeTriplet[Map[VertexId, Int], Int]) = {
    checkIncrement(e.srcAttr, e.dstAttr, e.dstId) ++
      checkIncrement(e.dstAttr, e.srcAttr, e.srcId)
  }


  def main(args : Array[String]): Unit ={
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //设置运行环境
    val conf = new SparkConf().setAppName("Facebook").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val topicGraph = GraphLoader.edgeListFile(sc, "facebook/facebook_combined.txt")
    topicGraph.cache()

    //println(topicGraph.vertices.collect.foreach(println))

    //7.6 度分布
    val degrees : VertexRDD[Int] = topicGraph.degrees.cache()
    //degrees.sortBy(_._2).map(l => l._1 + "\t" + l._2).saveAsTextFile("degrees_0")
    //sc.parallelize((0 until degrees.map(l => l._2).max)).map(l => (l, 0)).leftOuterJoin(degrees.map(l => (l._2, 1)).reduceByKey(_+_)).map(l => (l._1, l._2._2.getOrElse(0))).sortBy(_._1).map(l => l._1 + "\t" + l._2).saveAsTextFile("degree_dist_0")
    degrees.collect.foreach( l => println(l) )
    println(degrees.map(_._2).stats())


    //7.8小世界网络
    val triCountGraph = topicGraph.triangleCount()
    println(triCountGraph.vertices.map(x => x._2).stats())

    //7.8.1计算局部聚类系数
    val maxTrisGraph = topicGraph.degrees.mapValues( d => d * (d - 1)/2.0 )
    val clusterCoefGraph = triCountGraph.vertices.innerJoin(maxTrisGraph){
      (vertexId, triCount, maxTris) => {
        if(maxTris == 0) 0 else triCount/maxTris
      }
    }
    val avgClusterCoefGraph = clusterCoefGraph.map( l => l._2 ).sum/topicGraph.vertices.count
    println(avgClusterCoefGraph)


    //7.8.2平均路径长度
    val fraction =  0.02
    val replacement = false
    val sample = topicGraph.vertices.map( l => l._1 ).sample(replacement, fraction, 1729l)

    val ids = sample.collect.toSet
    val mapGraph = topicGraph.mapVertices( (id, _ ) => {
      if(ids.contains(id)){
        Map(id -> 0)
      }else{
        Map[VertexId, Int]()
      }
    })
    val start = Map[VertexId, Int]()
    val result = mapGraph.pregel(start)(update, iterate, mergeMaps)


    val paths = result.vertices.flatMap{ case(id, m) =>
      m.map{
        case(k,v) =>
          if(id < k){
            (id, k, v)
          }else{
            (k, id, v)
          }
      }
    }.distinct()
    paths.cache()
    println(paths.map(_._3).filter(_ > 0).stats())

    val hist = paths.map(_._3).countByValue()
    hist.toSeq.sorted.foreach(println)

    //7.9 计算pagerank
    val prResult = topicGraph.pageRank(0.1)
    prResult.vertices.sortBy(_._2,false).collect.foreach(println)
    println(prResult.vertices.map(l=>l._2).stats())

    //7.10计算closeness
    //7.10.1计算最小路径
   /* val finalRes = ArrayBuffer[(VertexId, Double)]()
    val userlist = topicGraph.vertices.collect
    for( user <- userlist){
      //println("找出" + user + "顶点的最短：")
      val initialGraph = topicGraph.mapVertices((id, _) => if (id == user._1) 0.0 else 10000000.0)
      val sssp = initialGraph.pregel(Double.PositiveInfinity)(
        (id, dist, newDist) => math.min(dist, newDist),
        triplet => {  // 计算权重
          if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
            Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
          } else {
            Iterator.empty
          }
        },
        (a,b) => math.min(a,b) // 最短距离
      )
      initialGraph.unpersistVertices(blocking = false)
      initialGraph.edges.unpersist(blocking = false)
      val result = sssp.vertices.map( v => {
        if(v._2 > 0.0 && v._2 < 10000000.0) {
          1/v._2
        }else{
          0.0
        }
      }).sum
      sssp.unpersistVertices(blocking = false)
      sssp.edges.unpersist(blocking = false)

      finalRes.+=((user._1, result))
      println(user._1 + " " + result)
    }

    val avgCloness = sc.parallelize(finalRes).map(l => l._2).sum/finalRes.size
    println("closenss : " + avgCloness)*/
  }
}