package com.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.storage.StorageLevel

object WordCountOnline {

  def main(args: Array[String]): Unit = {

    /**
     * 创建SparkConf
     */
    val conf = new SparkConf().setMaster("spark://node1:7077").setAppName("wordcountonline")

    val ssc = new StreamingContext(conf, Seconds(20))

    val lines = ssc.socketTextStream("node1", 7777, StorageLevel.MEMORY_AND_DISK)

    /**
     * 按空格分割
     */
    val words = lines.flatMap { line => line.split(" ") }

    /**
     * 把单个的word变成tuple
     */
    val counts = words.map { word => (word, 1) }.reduceByKey(_ + _)

    //wordCount.print()
    counts.saveAsTextFiles("/tmp/sparkStreaming")
    
    ssc.start()

    /**
     * 等待程序结束
     */
    ssc.awaitTermination()
    ssc.stop(true)

  }
}