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

class LojaTransform extends Transform {
  val log = LoggerFactory.getLogger(this.getClass)
  def transform(dataframe:DataFrame): DataFrame = {  
    dataframe.
    withColumn(
      SalesModel.Field.LST_SALES_ITEMS
      , dropUseless(col(SalesModel.Field.LST_SALES_ITEMS))
    ).
    withColumn("salt", (rand * ConfigUtil.getSparkPartitions).cast(IntegerType)).
    repartition(ConfigUtil.getSparkPartitions, col(SalesModel.Field.COD_SITE), col("salt"))
  }
}

