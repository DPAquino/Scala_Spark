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

class ClienteCadastradoTransform extends Transform {
  val log = LoggerFactory.getLogger(this.getClass)
  def transform(dataframe:DataFrame): DataFrame = {  
    dataframe.
    where(col(SalesModel.Field.IND_REGISTERED) === "1").
    withColumn(
      SalesModel.Field.LST_SALES_ITEMS
      , dropUseless(col(SalesModel.Field.LST_SALES_ITEMS))
    ).
    repartition(ConfigUtil.getSparkPartitions, col(SalesModel.Field.NUM_CONSUMER_DOCUMENT_PRIORITY))
  }
}

