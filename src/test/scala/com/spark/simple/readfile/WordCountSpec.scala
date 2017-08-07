/*
package com.spark.simple.readfile

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.mockito.MockitoSugar
import org.scalatest.FlatSpec
import org.mockito.Mockito._
import org.mockito.Matchers._

class WordCountSpec extends FlatSpec with WordCount with MockitoSugar {


  val mocksc = mock[SparkContext]
  val mockRDDString = mock[RDD[String]]
  when(mocksc.textFile(any[String], any[Int])).thenReturn(mockRDDString)
  //when(mockRDDString.cache()).thenReturn(any[RDD[String]])

  "For the word count it" should "cache the file read" in {
    val wc = new WordCount {}
    // wc.readLines(mocksc, "anypath")
    verify(mocksc, times(1)).textFile(any[String], any[Int])
    verify(mockRDDString, times(1)).cache()
  }


}
*/
