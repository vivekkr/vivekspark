package com.hsbc.gbm.mads.tradeunpack.config

object Keys {
  val SparkMaster = "localhost[*]"
  val SparkAppUTCTradeHiveToEs = "UTCTrade Hive to ES Writer"
  val SparkSQLWarehouseDir = "spark.sql.warehouse.dir"
  val config = Map(
    SparkSQLWarehouseDir -> "target/spark-warehouse"
  )

  val hiveKerberosKeytab = "hive.metastore.kerberos.keytab.file"
  val hiveKerberosPrincipal = "hive.metastore.kerberos.principal"
  val hiveKerberosSasl = "hive.metastore.sasl.enabled"
  val hiveKerberosUrls = "hive.metastore.urls"
  val hiveKerberosAuthentication = "hive.server2.authentication"

  val hiveConfig = Map(
    hiveKerberosKeytab -> "",
    hiveKerberosPrincipal -> "",
    hiveKerberosSasl -> "",
    hiveKerberosUrls -> "",
    hiveKerberosAuthentication -> ""
  )

  val esNodes = "es.nodes"
  val esPort = "es.port"
  val esAuthUser = "es.net.http.auth.user"
  val esAuthPasswd = "es.net.http.auth.pass"
  val esIndexAutoCreate = "es.index.auto.create"
}