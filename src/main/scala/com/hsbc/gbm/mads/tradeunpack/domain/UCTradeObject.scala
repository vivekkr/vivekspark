package com.hsbc.gbm.mads.tradeunpack.domain

case class UCTradeObject(dslObjectType: String, sourceSystem: String, businessDate: String, objectId: String, version: Int, xml: XML)
