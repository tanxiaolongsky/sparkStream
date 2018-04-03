package com.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{ Seconds, StreamingContext }

/**
 *
 * 基于滑动窗口的热点搜索词实时统计
 * 每隔5秒钟，统计最近20秒钟的搜索词的搜索频次，
 * 并打印出排名最靠前的3个搜索词以及出现次数
 *
 */
object WindowDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("spark://node1:7077").setAppName("WindowDemo")
    val ssc = new StreamingContext(conf, Seconds(5))

    //从nc服务中获取数据,数据格式：username searchword，比如：张三 大数据
    val linesDStream = ssc.socketTextStream("node1", 7777)
    //将数据中的搜索词取出
    val wordsDStream = linesDStream.map(_.split(" ")(1))
    //通过map算子，将搜索词形成键值对(word,1),将搜索词记录为1次
    val searchwordDStream = wordsDStream.map(searchword => (searchword, 1))
    //通过reduceByKeyAndWindow算子,每隔5秒统计最近20秒的搜索词出现的次数
    val reduceDStream = searchwordDStream.reduceByKeyAndWindow(
      (v1: Int, v2: Int) =>
        v1 + v2, Seconds(86400), Seconds(5))
    //调用DStream中的transform算子,可以进行数据转换
    val transformDStream = reduceDStream.transform(searchwordRDD => {
      val result = searchwordRDD.map(m => {
        //将key与value互换位置
        (m._2, m._1)
      }).sortByKey(false) //根据key进行降序排列
        .map(m => { //将key与value互换位置
          (m._2, m._1)
        }).take(3) //取前3名

      for (elem <- result) {
        println(elem._1 + "  " + elem._2)
      }
      searchwordRDD //注意返回值
    })
    transformDStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}