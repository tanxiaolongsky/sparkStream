package com.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.storage.StorageLevel
/*
 * 计算每隔5s 统计最近1分钟的销售额
 * 数据传输格式 编号：价格 such as 001：100
 */
object slipWindow {
  def main(args: Array[String]): Unit = {
    //sparkConf
    var conf = new SparkConf().setMaster("spark://node1:7077").setAppName("slipWindow")
    // sc
    var ssc = new StreamingContext(conf, Seconds(5))
    // data
    var dstream = ssc.socketTextStream("node1", 7777, StorageLevel.MEMORY_AND_DISK)

    // 1 min sum(price)
    var windowDs = dstream.window(Seconds(60), Seconds(5))

    // print Dstream RDD number
    //print(windowDs.count())

    // just keep price
    var priceDStream = windowDs.map(_.split(":")(1))

    var priceDs = priceDStream.map(price => (1, Integer.parseInt(price)))

    var sumPrice = priceDs.reduceByKey(_ + _)

    sumPrice.print()
    
    ssc.start()
    ssc.awaitTermination()

  }

}