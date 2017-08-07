package com.spark.ccy.reader

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

class CcyReaderSpec extends FlatSpec with DataFrameSuiteBase {

  "CcyReader" should "return the expected data from the file" in {

    val data = CcyReader.readDataAndReturnSchema(spark, "src/test/resources/ccy_trade_data.txt")
    data.collect().count(_.percent > 0) shouldBe 23
  }
}
