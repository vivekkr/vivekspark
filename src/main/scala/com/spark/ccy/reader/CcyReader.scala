package com.spark.ccy.reader

import org.apache.spark.sql.types._
import org.apache.spark.sql._

/**
  * Created by vivekkr on 02/08/2017.
  */

case class CCYTrade(category: String, customerId: Int, localDate: String, x: String, y: String, z: String, percent: Int)

object CcyReader {

  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder
      .appName("CCY Aggregator")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "target/ccy-reader")
      .getOrCreate

    import sparkSession.implicits._

    //CNYHKD, 1, 2017-12-09, X, Y, Z,19

    val ccyTradeDS = readDataAndReturnSchema(sparkSession, "src/test/resources/ccy_trade_data.txt")

    val groupByCategory = ccyTradeDS.groupByKey(_.category)
    groupByCategory.keys.show
    val reducedValue: Dataset[(String, Int)] = groupByCategory.mapValues(_.percent).reduceGroups(_ + _)
    reducedValue.foreach(println(_))

    val groupById = ccyTradeDS.groupByKey(_.customerId)
    groupById.keys.show
    val reducedValueById: Dataset[(Int, Int)] = groupById.mapValues(_.percent).reduceGroups(_ + _)
    reducedValueById.foreach(println(_))


/*
    val innerSchema = StructType(
      Array(
        StructField("value", StringType),
        StructField("count", LongType)
      )
    )

    val outputSchema =
      StructType(
        Array(
          StructField("name", StringType, nullable = false),
          StructField("index", IntegerType, nullable = false),
          StructField("count", LongType, nullable = false),
          StructField("empties", LongType, nullable = false),
          StructField("nulls", LongType, nullable = false),
          StructField("uniqueValues", LongType, nullable = false),
          StructField("mean", DoubleType),
          StructField("min", DoubleType),
          StructField("max", DoubleType),
          StructField("topValues", innerSchema)
        )
      )

    val stats = Seq("Zero", 1, 2, 3, 4, 5, 6, 7, 8, Array("some value", 9))

    val result: Seq[Row] = stats.map { c =>
      Row(stats(0), stats(1), stats(2), stats(3), stats(4), stats(5), stats(6), stats(7), stats(8), stats(9))
    }

    val rdd = sparkSession.sparkContext.parallelize(result.toSeq)

    val outputDf = sparkSession.sqlContext.createDataFrame(rdd, outputSchema)

    outputDf.show()
*/

    sparkSession.stop()
  }

  def readDataAndReturnSchema(sparkSession: SparkSession, fileLocation: String) = {
    import sparkSession.implicits._

    val row = sparkSession.read.text(fileLocation).as[String]
    row.collect().foreach(println)

    val ccyTradeDS: Dataset[CCYTrade] = row.map(r => r.split(",").map(_.trim)).map {
      case fields: Array[_] => CCYTrade(fields(0), fields(1).toInt, fields(2), fields(3), fields(4), fields(5), fields(6).toInt)
      /*
        case Array(category: String, customerId: String, localDate: String, x: String, y: String, z: String, percent: String) =>
          CCYTrade(category, customerId.toInt, localDate, x, y, z, percent.toInt)
      */
    }
    ccyTradeDS
  }

}
