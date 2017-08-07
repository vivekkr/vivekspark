package com.coursera.spark.wk3

import com.holdenkarau.spark.testing.SharedSparkContext
import domain.CFFPurchase
import org.apache.spark.rdd.RDD
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import scala.io.Source

class ReduceByKeyRDDSpec extends FlatSpec with SharedSparkContext {

  val csvFile = "src/test/resources/stackoverflow.csv"
  val sampleCSVFile = "src/test/resources/stackoverflow_sample.csv"

  val purchaseList = List(
    CFFPurchase(100, "Geneva", 22.25),
    CFFPurchase(100, "Lucerne", 31.60),
    CFFPurchase(100, "Friborough", 12.40),
    CFFPurchase(200, "St.Gallen", 8.20),
    CFFPurchase(200, "Dubendorf", 23.15),
    CFFPurchase(300, "Zurich", 42.10),
    CFFPurchase(300, "Basel", 16.20)
  )
  "A simple transformation" should "be successful" in {
    val purchasesRDD: RDD[(Int, Double)] = sc.parallelize(purchaseList).map(p => (p.customerId, p.price))
    val cffCollection = purchasesRDD.collect()
    cffCollection shouldNot be(Array.empty)

    //totalPrice.foreach(println)

    val totalPrice: RDD[(Int, Double)] = purchasesRDD.reduceByKey(_ + _)
    val purchaseRDDMap: RDD[(Int, CFFPurchase)] = sc.parallelize(purchaseList).map(p => (p.customerId, p)).persist()
    val aggregatedValue = purchaseRDDMap.aggregateByKey(0.0)((a, c1) => a + c1.price, (a, b) => a + b)
    val t1 = System.currentTimeMillis()
    val totalTrips = purchaseRDDMap.aggregateByKey(0)((a, c1) => a + 1, (trip1, trip2) => trip1 + trip2)

    val t2 = System.currentTimeMillis()
    println(s"Time Taken:  " + (t2 - t1))

  }

  it should "read 2000 lines from the full csv" in {
    import scala.runtime.ScalaRunTime._
    val startTime = System.currentTimeMillis()
    val fileRDD: RDD[String] = sc.textFile(csvFile)
    fileRDD.count() shouldBe 8143801
    fileRDD.collect() foreach { l =>
      val c = l.split(",")
      //println(stringOf(c))
      //println(c.mkString(","))
    }

    fileRDD.collect() foreach { l =>
      val c = l.split(",")
      // println(stringOf(c))
      // println(c.mkString(","))
    }
    val endTime = System.currentTimeMillis()
    println(s"Time Taken: ${endTime - startTime}")
  }


  "For the Sample csv, it" should "For each collector get the language and the count" in {

    val sampleRDD = sc.textFile(sampleCSVFile)
    val pairRDD: RDD[(String, String)] = sampleRDD map { l =>
      val lineArray = l.split(",")
      lineArray.length match {
        case 6 => (lineArray(0), lineArray(5))
        case _ => (lineArray(0), "No Value")
      }
    }

    val pairedById: RDD[(String, Iterable[String])] = pairRDD.groupByKey()


    val langTuple: RDD[(String, Iterable[(String, Int)])] = pairedById.mapValues(x => {
      x.map { y =>
        (y, 1)
      }
    })

    val newT: RDD[(String, Map[String, Int])] = langTuple mapValues ((x: Iterable[(String, Int)]) => x.groupBy(_._1).mapValues(_.map(_._2).sum))
    //println("Valid RDD size is " + newT.collect().size)
    newT foreach { (c: (String, Map[String, Int])) =>
      c._2 foreach { m =>
        println(s"For the collector : ${c._1} and Language: ${m._1} the count is ${m._2}")
      }
    }
  }


  it should "get the id, language and the count - version 2" in {

    val sampleRDD = sc.textFile(sampleCSVFile)
    val arrayRDD: RDD[Array[String]] = sampleRDD map { x =>
      x.split(",")
    }
    val filteredRDD = arrayRDD.filter(_.size > 5)
    println("Valid RDD size is " + filteredRDD.collect().size)
    //filteredRDD.collect().foreach(a => println(a.mkString(",")))

    val pairRDD: RDD[((String, String), Int)] = filteredRDD map {
      case arr => ((arr(0), arr(5)), 1) //arr(4).toInt)
    }

    pairRDD.groupByKey().mapValues(_.sum).foreach { c =>
      println(s"For the collector : ${c._1._1} and Language: ${c._1._2} the count is ${c._2}")
    }
  }

}


