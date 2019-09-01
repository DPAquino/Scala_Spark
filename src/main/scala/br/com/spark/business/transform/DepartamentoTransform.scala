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

class DepartamentoTransform extends Transform {
  val log = LoggerFactory.getLogger(this.getClass)
  def transform(dataframe:DataFrame): DataFrame = {  
    dataframe.
    withColumn(
      SalesModel.Field.LST_SALES_ITEMS
      , dropUseless(col(SalesModel.Field.LST_SALES_ITEMS))
    ).
    repartition(ConfigUtil.getSparkPartitions).
    withColumn(
      SalesModel.Field.LST_SALES_ITEMS
      , explode(col(SalesModel.Field.LST_SALES_ITEMS))
    ).withColumn(
      SalesModel.ItemField.DS_DEPARTMENT
      , col(s"${SalesModel.Field.LST_SALES_ITEMS}.${SalesModel.ItemField.DS_DEPARTMENT}")
    ).
    groupBy(
      col(SalesModel.ItemField.DS_DEPARTMENT)
      , col(SalesModel.Field.ID_SOURCE_SALE)
      , col(SalesModel.Field.COD_UF)
      , col(SalesModel.Field.DS_SITE_FORMAT)
      , col(SalesModel.Field.COD_IND_SITE_ECOMM)
      , col(SalesModel.Field.COD_DELIVERY)
      , col(SalesModel.Field.VAL_DOCUMENT_TOTAL_WITH_SERVICES)
      , col(SalesModel.Field.QTY_TOTAL_COUPON_BURNT)
      , col(SalesModel.Field.IND_IDENTIFIED)
      , col(SalesModel.Field.IND_REGISTERED)
    ).
    agg(collect_list(col(SalesModel.Field.LST_SALES_ITEMS)).alias(SalesModel.Field.LST_SALES_ITEMS))
  }
}

