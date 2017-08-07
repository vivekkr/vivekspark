package com.hsbc.gbm.mads.tradeunpack.app

import java.nio.file.{Files, Paths}

import com.holdenkarau.spark.testing.DatasetSuiteBase
import com.hsbc.gbm.mads.tradeunpack.domain.{UCTradeObject, XML}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import scala.reflect.io.File


class HiveToEsAppSpec extends FlatSpec with DatasetSuiteBase {

  private val readFile = "src/test/resources/uc_trade_data.txt"

  "HiveToEs" should "have the correct schema for the UCTrade data" in {

    val xmlStruct = StructType(
      StructField("name", StringType) ::
        StructField("tradeXML", StringType) :: Nil)

    val expectedSchema = new StructType()
      .add("dslObjectType", StringType)
      .add("sourceSystem", StringType)
      .add("businessDate", StringType)
      .add("objectId", StringType)
      .add("version", IntegerType)
      .add("xml", xmlStruct
      )

    HiveToEsApp.readFileData(spark, readFile).schema shouldBe expectedSchema
  }

  it should "transform the UCTrade Data correctly" in {

    val expected = Array(UCTradeObject("UC_TRADE", "NFOS", "2017-12-05", "Michael", 29,
      XML("Michael", "<?xml version=\"1.0\"?><trades><trade id=\"a1\"><objecttype>UC_TRADE</objecttype><price>10</price></trade></trades>")
    ))

    HiveToEsApp.readFileData(spark, readFile).take(1) shouldBe expected
  }

  it should "save the UCTrade Data at the supplied path" in {

    val saveLocation = "orcwriter/output/orc"

    val dataSet = HiveToEsApp.readFileData(spark, readFile)
    File(saveLocation).deleteRecursively

    HiveToEsApp.saveUCTradeObjectAsOrc(dataSet, saveLocation)
    Files.exists(Paths.get(saveLocation)) shouldBe true
  }

  it should "read the ORC Data from the supplied path" in {


    val readLocation = "orcwriter/output/orc"
    val expectedUCTradeObject = UCTradeObject("UC_TRADE", "NFOS", "2017-12-05", "Michael", 29,
      XML("Michael", "<?xml version=\"1.0\"?><trades><trade id=\"a1\"><objecttype>UC_TRADE</objecttype><price>10</price></trade></trades>")
    )

    import spark.implicits._

    val expectedUCTradeDS = spark.createDataset(Seq(expectedUCTradeObject))
    val ucDS = HiveToEsApp.readUCTradeObjectAsOrc(spark, readLocation)

    ucDS shouldNot be(expectedUCTradeDS)
  }

  it should "save the data to the Elastic" in {
    val ucDS = HiveToEsApp.readFileData(spark, readFile)
    HiveToEsApp.saveUCTradeToElastic(spark, ucDS)
  }

}
