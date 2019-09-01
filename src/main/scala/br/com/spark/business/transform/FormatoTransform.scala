package br.com.spark.business.transform

// built-in
import org.slf4j.LoggerFactory
// third party
import org.apache.spark.sql.{SparkSession, Column, DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
// local
import br.com.spark.model.SalesModel
import br.com.spark.utils.ConfigUtil

class FormatoTransform extends Transform {
  val log = LoggerFactory.getLogger(this.getClass)
  def transform(dataframe:DataFrame): DataFrame = {  
    dataframe.
    withColumn(
      SalesModel.Field.LST_SALES_ITEMS
      , dropUseless(col(SalesModel.Field.LST_SALES_ITEMS))
    ).withColumn(
      SalesModel.Field.DS_SITE_FORMAT
      , getkpiGroup(col(SalesModel.Field.DS_SITE_FORMAT))
    ).
    withColumn("salt", (rand * ConfigUtil.getSparkPartitions).cast(IntegerType)).
    repartition(ConfigUtil.getSparkPartitions, col(SalesModel.Field.DS_SITE_FORMAT), col("salt")).
    select(
      col(SalesModel.Field.NUM_CONSUMER_DOCUMENT_PRIORITY)
      , col(SalesModel.Field.COD_SITE)
      , col(SalesModel.Field.COD_UF)
      , col(SalesModel.Field.ID_SOURCE_SALE)
      , explode(col(SalesModel.Field.DS_SITE_FORMAT)).alias(SalesModel.Field.DS_SITE_FORMAT)
      , col(SalesModel.Field.COD_IND_SITE_ECOMM)
      , col(SalesModel.Field.LST_SALES_ITEMS)
      , col(SalesModel.Field.COD_DELIVERY)
      , col(SalesModel.Field.VAL_DOCUMENT_TOTAL_WITH_SERVICES)
      , col(SalesModel.Field.QTY_TOTAL_COUPON_BURNT)
      , col(SalesModel.Field.IND_IDENTIFIED)
      , col(SalesModel.Field.IND_REGISTERED)
    )
  }
}

