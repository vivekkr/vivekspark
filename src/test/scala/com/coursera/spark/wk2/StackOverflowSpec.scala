package com.coursera.spark.wk2

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.{Partitioner, RangePartitioner}
import org.scalatest.Matchers._
import org.scalatest.{BeforeAndAfterEach, FlatSpec}

import scala.util.Try

class StackOverflowSpec extends FlatSpec with SharedSparkContext with BeforeAndAfterEach {

  var stackOverFlow: StackOverflow = _
  var startTime: Long = _

  override def beforeEach(): Unit = {
    startTime = System.currentTimeMillis()
    stackOverFlow = StackOverflow("src/test/resources/stackoverflow.csv", sc)
  }

  "Reading the file from Stackoverflow" should "be successful and return correct partitions" in {
    val lines = stackOverFlow.readLines
    lines.take(100) foreach println
    val finalTime = System.currentTimeMillis()
    val numberOfPartitions = lines.getNumPartitions
    numberOfPartitions shouldBe 8
    println(s"Partitions: $numberOfPartitions Time Taken: " + (finalTime - startTime))
  }

  "The raw posting entries" should "return data for each line" in {
    val lines = stackOverFlow.readLines
    val postingRDD = lines map { l =>
      val cols: Array[String] = l.split(",")
      Posting(cols(0).toInt, cols(1).toInt, Try(cols(2).toInt).toOption, Try(cols(3).toInt).toOption, cols(4).toInt, Try(cols(5).toString).toOption)
    }

    println(postingRDD.toDebugString) // To suggest the kind of RDD's involved
    println(postingRDD.dependencies) // To Suggest Narrow or WideDependencies (like ShuffleDependency)
    postingRDD.isEmpty() shouldBe false
    postingRDD.count() shouldBe 8143801
    println(s"Time Taken: " + (System.currentTimeMillis - startTime))

    /**
      * MappedRDD at subtract (3 partitions)
      * SubtractedRDD at subtract(3 partitions)
      * MappedRDD at subtract(3 partitions)
      * ParallelCollectionRDD at parallelize(3 partitions)
      */
  }


  "A Custom partitioner" should "return data for each line" in {
    val lines = stackOverFlow.readLines
    val myPairRDD = lines map { l =>
      val cols: Array[String] = l.split(",")
      (cols(0).toInt, Posting(cols(0).toInt, cols(1).toInt, Try(cols(2).toInt).toOption, Try(cols(3).toInt).toOption, cols(4).toInt, Try(cols(5).toString).toOption))
    }

    val myRangePartitioner = new RangePartitioner(8, myPairRDD) //'myPairRDD' needs to be passed here so that the partitioner can sample the RDD for best ranges
    val partitionedRDD = myPairRDD.partitionBy(myRangePartitioner).persist() //Must persist
    partitionedRDD.count() shouldBe 8143801
    println(s"Time Taken: " + (System.currentTimeMillis - startTime))
  }

  "Test run must wait " should "the executor" in {
    val historyCheck  = sc.parallelize(1 to 100)
    historyCheck.count shouldBe 100
  }

}
