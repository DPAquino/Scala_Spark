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
import br.com.spark.business.SalesMetricClassifier

class ServicoTransform extends Transform {
  val log = LoggerFactory.getLogger(this.getClass)
  val getService = udf(
      (id_source_sale: Int, cod_ind_site_ecomm: Int, cod_delivery: Int, cod_bus: String) => 
      {
        if(id_source_sale == 2) {
          val ecm_std = (cod_ind_site_ecomm == 1 && (cod_bus == "NFOOD" && Seq(0,1,2,3).contains(cod_delivery)) || (cod_bus == "FOOD" && cod_delivery == 3))
          val click   = (cod_ind_site_ecomm == 1 && cod_bus == "NFOOD" && Seq(4,5).contains(cod_delivery))
          val drive   = (cod_ind_site_ecomm == 1 && cod_bus == "FOOD" && cod_delivery == 4)
          val scan    = (cod_ind_site_ecomm == 0 && cod_delivery == 6)
          val rappi   = (cod_ind_site_ecomm == 1 && cod_bus == "FOOD" && cod_delivery == 7)
          if (ecm_std) {
            "ecm_std"
          } else if(click) {
            "click"
          } else if(drive) {
            "drive"
          } else if(scan) {
            "scan"
          } else if(rappi) {
            "rappi"
          } else {
            "ecm_other"
          }
        } else {
          null
        }
      }
  )

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
      "servico"
      , getService(
          col(SalesModel.Field.ID_SOURCE_SALE).cast(IntegerType)
          , col(SalesModel.Field.COD_IND_SITE_ECOMM).cast(IntegerType)
          , col(SalesModel.Field.COD_DELIVERY).cast(IntegerType)
          , col(s"${SalesModel.Field.LST_SALES_ITEMS}.${SalesModel.ItemField.COD_BUS}").cast(StringType)
        )
    ).
    groupBy(
      col("servico")
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

