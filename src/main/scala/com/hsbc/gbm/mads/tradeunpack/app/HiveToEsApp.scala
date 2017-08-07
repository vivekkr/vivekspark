package com.hsbc.gbm.mads.tradeunpack.app

import com.hsbc.gbm.mads.tradeunpack.config.Keys._
import com.hsbc.gbm.mads.tradeunpack.domain.{UCTradeObject, XML}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

object HiveToEsApp {

  private val logger = LoggerFactory.getLogger(HiveToEsApp.getClass)

  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder
      .appName(SparkAppUTCTradeHiveToEs)
      .master(SparkMaster)
      .enableHiveSupport()
      .config(SparkSQLWarehouseDir, config(SparkSQLWarehouseDir))
      .config(esNodes, "localhost")
      .config(esPort, "9200")
      .config(esIndexAutoCreate, "true")
      .getOrCreate

    val ds = readFileData(sparkSession, "src/test/resources/uc_trade_data.txt")
    saveUCTradeObjectAsOrc(ds, "orcwriter/output/orc", "businessDate")
    saveUCTradeToElastic(sparkSession, ds)

    sparkSession.close()
  }

  def readFileData(sparkSession: SparkSession, folderToRead: String): Dataset[UCTradeObject] = {

    import sparkSession.implicits._

    //Creating DS by hand
    val data: Dataset[String] = sparkSession.read.text(folderToRead).as[String]
    data.map { rowAsStr =>
      val elements = rowAsStr.split(",").map(_.trim)
      UCTradeObject(elements(0), elements(1), elements(2), elements(3), elements(4).toInt, XML(elements(3), elements(5)))
    }
  }

  def saveUCTradeObjectAsOrc(ds: Dataset[UCTradeObject], outputLocation: String, partition: String*): Unit = {
    ds.write.mode(SaveMode.Overwrite).partitionBy(partition: _*).orc(outputLocation)
  }

  def readUCTradeObjectAsOrc(sparkSession: SparkSession, readLocation: String): Dataset[UCTradeObject] = {

    import sparkSession.implicits._

    val ucDF = sparkSession.read.orc(readLocation)
    ucDF.printSchema()
    ucDF.as[UCTradeObject]
  }

  def saveUCTradeToElastic(spark: SparkSession, ucDS: Dataset[UCTradeObject]) = {

    import org.elasticsearch.spark.sql._

    ucDS.toDF().saveToEs("spark/uctradedata")
  }
}
