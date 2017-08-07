package com.spark.simple.readfile

import org.apache.spark.{SparkConf, SparkContext}

trait WordCount {

  def createSparkContext = {
    val conf = new SparkConf().setAppName("Read File").setMaster("local[2]").set("spark.executor.memory", "512m");
    val sc = new SparkContext(conf)
    readLines(sc, "src/main/resources/file1.txt")
  }

  def readLines(sc: SparkContext, path: String) {
    val logData = sc.textFile(path, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    println(s"Lines with a: $numAs")
    sc.stop()
  }

}
