package com.jmdmajid.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object FriendsAvgByAge {
  def parseLine(line: String) = {
    val features = line.split(",")
    val age = features(2).toInt
    val friends = features(3).toInt
    (age, friends)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "AverageFiends")
    val lines = sc.textFile("data/fakefriends.csv")
    val rdd = lines.map(parseLine)

    val  totalByAge = rdd.mapValues(x => (x,1))
      .reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))

    val avgByAge = totalByAge.mapValues(x => x._1 / x._2)
    val final_result = avgByAge.collect()

    final_result.sorted.foreach(println)
  }
}
