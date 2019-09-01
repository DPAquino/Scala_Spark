package br.com.spark.business

// built-in
import org.slf4j.LoggerFactory
// thrid party
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SparkSession, Column, DataFrame, Row}
// local
import br.com.spark.dao._
import br.com.spark.model._
import br.com.spark.utils._
import br.com.spark.business.transform._

class SalesMetricBusiness(date: String, view: String) {
  val log = LoggerFactory.getLogger(this.getClass)
  val salesDao = new SalesDao()
  val metricsDao = new MetricsDao()
  
  def getSalesAnalyticsStore(spark: SparkSession): DataFrame = {
    salesDao.getByMonth(spark, date)
  }
 
  def getMetricUDAF(classUDAF: String): UserDefinedAggregateFunction = {
    Class.forName(s"br.com.spark.business.rules.${classUDAF}").newInstance().asInstanceOf[UserDefinedAggregateFunction]
  }

  def getTransformInstance: Transform = {
    Class.forName(s"br.com.spark.business.transform.${ConfigUtil.code_view(view)}").newInstance().asInstanceOf[Transform]
  }
  
  def applyMetric(df: DataFrame, grp_metric:String): DataFrame = {
    val metricDF = 
      df.groupBy(getMetricViewFields:_*).
      agg(getMetricUDAF(ConfigUtil.code_grp_metric(grp_metric))(UdafModel.ClientSchema.fieldNames.map(i => col(i)).toSeq: _*) as "metric")
    
    metricDF.
    withColumn(MetricsModel.Field.ID_METRIC, lit(grp_metric)).
    withColumn(MetricsModel.Field.DS_METRIC, lit(ConfigUtil.desc_grp_metric(grp_metric))).
    withColumn(MetricsModel.Field.ID_VIEW_DOCUMENT, lit(if (view == null) 1 else view.toInt)).
    withColumn(MetricsModel.Field.DS_VIEW_DOCUMENT, lit(ConfigUtil.desc_view(if (view == null) "1" else view )))
  }

  def getResult(df: DataFrame): DataFrame = {
    val getkpi = udf((xs: String) => try{ ConfigUtil.desc_metric(xs) } catch { case e:Exception => "" })
    log.info(s"[*] View Id: ${view} | View Fields: ${getMetricViewFields.map(i => i.toString).mkString(",")}")
  
    df.select(
      col(MetricsModel.Field.NUM_DOCUMENT)
      , col(MetricsModel.Field.COD_UF)
      , col(MetricsModel.Field.ID_VIEW_DOCUMENT)
      , col(MetricsModel.Field.DS_VIEW_DOCUMENT)
      , col(MetricsModel.Field.ID_METRIC)
      , col(MetricsModel.Field.DS_METRIC)
      , posexplode(col("metric"))
    ).
    withColumn(MetricsModel.Field.DS_KPI, getkpi(col("pos"))).
    withColumn(MetricsModel.Field.DTH_INGESTION, current_timestamp()).
    withColumn(MetricsModel.Field.COD_YEAR, lit(DateUtil.getYear(date))).
    withColumn(MetricsModel.Field.COD_MONTH, lit(DateUtil.getMonth(date))).
    withColumn(MetricsModel.Field.ID_KPI, col("pos")).
    withColumn(MetricsModel.Field.VAL_KPI, col("col")).
    select(
       col(MetricsModel.Field.ID_VIEW_DOCUMENT).cast(IntegerType)
      , col(MetricsModel.Field.DS_VIEW_DOCUMENT).cast(StringType)
      , col(MetricsModel.Field.NUM_DOCUMENT).cast(StringType)
      , col(MetricsModel.Field.COD_UF).cast(StringType)
      , col(MetricsModel.Field.DS_METRIC).cast(StringType)
      , col(MetricsModel.Field.ID_KPI).cast(IntegerType)
      , col(MetricsModel.Field.DS_KPI).cast(StringType)
      , col(MetricsModel.Field.VAL_KPI).cast(DecimalType(38,10))
      , col(MetricsModel.Field.DTH_INGESTION).cast(TimestampType)
      , col(MetricsModel.Field.COD_YEAR).cast(IntegerType)
      , col(MetricsModel.Field.COD_MONTH).cast(IntegerType)
      , col(MetricsModel.Field.ID_METRIC).cast(IntegerType)
    )
  }

  def saveMetrics(spark:SparkSession, dataFrame: DataFrame, date:String, paramView:String): Unit = {
    metricsDao.save(spark, dataFrame, date, paramView)
  }

  def getMetricViewFields():Seq[Column] = view match {
    case "1" | "11" => 
      Seq(
        col(SalesModel.Field.NUM_CONSUMER_DOCUMENT_PRIORITY).cast(LongType).cast(StringType).alias(MetricsModel.Field.NUM_DOCUMENT)
        , col(SalesModel.Field.COD_UF).cast(IntegerType).alias(MetricsModel.Field.COD_UF)
      )
    case "2" => 
      Seq(
        col(SalesModel.Field.COD_SITE).cast(IntegerType).cast(StringType).alias(MetricsModel.Field.NUM_DOCUMENT)
        , col(SalesModel.Field.COD_UF).cast(IntegerType).alias(MetricsModel.Field.COD_UF)
      )
    case "3" => 
      Seq(
        col(SalesModel.ItemField.DS_SECTOR).cast(StringType).alias(MetricsModel.Field.NUM_DOCUMENT)
        , col(SalesModel.Field.COD_UF).cast(IntegerType).alias(MetricsModel.Field.COD_UF)
      )
    case "4" => 
      Seq(
        col(SalesModel.ItemField.DS_DEPARTMENT).cast(StringType).alias(MetricsModel.Field.NUM_DOCUMENT)
        , col(SalesModel.Field.COD_UF).cast(IntegerType).alias(MetricsModel.Field.COD_UF)
      )
    case "5" => 
      Seq(
        col(SalesModel.ItemField.DS_GROUP).cast(StringType).alias(MetricsModel.Field.NUM_DOCUMENT)
        , col(SalesModel.Field.COD_UF).cast(IntegerType).alias(MetricsModel.Field.COD_UF)
      )
    case "6" => 
      Seq(
        upper(col(SalesModel.Field.DS_SITE_FORMAT).cast(StringType)).alias(MetricsModel.Field.NUM_DOCUMENT)
        , col(SalesModel.Field.COD_UF).cast(IntegerType).alias(MetricsModel.Field.COD_UF)
      ) 
    case "7" => 
      Seq(
        col("servico").cast(StringType).alias(MetricsModel.Field.NUM_DOCUMENT)
        , col(SalesModel.Field.COD_UF).cast(IntegerType).alias(MetricsModel.Field.COD_UF)
      ) 
    case "8" => 
      Seq(
        col(SalesModel.Field.COD_UF).cast(IntegerType).cast(StringType).alias(MetricsModel.Field.NUM_DOCUMENT)
        , col(SalesModel.Field.COD_UF).cast(IntegerType).alias(MetricsModel.Field.COD_UF)
      )
    case _ => 
      Seq(
        col(SalesModel.Field.NUM_CONSUMER_DOCUMENT_PRIORITY).cast(LongType).cast(StringType).alias(MetricsModel.Field.NUM_DOCUMENT)
        , col(SalesModel.Field.COD_UF).cast(IntegerType).alias(MetricsModel.Field.COD_UF)
      )
  }

}

object SalesMetricBusiness {
  val log = LoggerFactory.getLogger(this.getClass)
  var instance: SalesMetricBusiness = null

  def getInstance(date: String, view: String) = {
    log.info("[*] Getting instance")
    if (instance == null)
      instance = new SalesMetricBusiness(date, view)
    instance
  }
}
