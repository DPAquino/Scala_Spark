package br.com.spark.service

// built-in
import org.slf4j.LoggerFactory
// thrid party
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
// local
import br.com.spark.business.SalesMetricBusiness
import br.com.spark.utils._

object SalesService {
  val log = LoggerFactory.getLogger(this.getClass)

  def saveMetric(spark:SparkSession, date:String, metrics:String, paramView:String) = {
    
    val businessInstance: SalesMetricBusiness = SalesMetricBusiness.getInstance(date, paramView)
    val sales_analytics_store: DataFrame = businessInstance.getSalesAnalyticsStore(spark)
    val dataFrame: DataFrame = businessInstance.getTransformInstance.transform(sales_analytics_store)

    val list_metrics: String = if (metrics.isEmpty) { 
      ConfigUtil.code_grp_metric.map{m => m._1.toInt}.toSeq.sorted.map{m => s"${m}"}.mkString(",")
    } else { metrics }

    val dfMetrics: DataFrame = {
      val dfReturn: Seq[DataFrame] = list_metrics.split(",").map{
        x => {
          log.info(s"[*] Executing metric: ${ConfigUtil.code_grp_metric(x)}")
          businessInstance.applyMetric(dataFrame, x)
        }
      }
      businessInstance.getResult(dfReturn.reduce(_.union(_)))
    }

    log.info(s"[*] Saving to Hive")
    businessInstance.saveMetrics(spark, dfMetrics, date, paramView)
  }
}
