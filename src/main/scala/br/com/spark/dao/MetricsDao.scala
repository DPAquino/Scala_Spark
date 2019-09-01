package br.com.spark.dao

// built-in
// thrid party
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
// local
import br.com.spark.model.MetricsModel
import br.com.spark.utils.DateUtil

class MetricsDao {

  val log = LoggerFactory.getLogger(this.getClass)

  def getAll(spark: SparkSession): DataFrame = {
    spark.table(MetricsModel.DB_TABLE)
  }

  def getByDay(spark: SparkSession, date: String): DataFrame = {
    getByMonth(spark, date)
  }

  def getByMonth(spark: SparkSession, date: String): DataFrame = {
    spark.sql(
      s"""SELECT * FROM ${MetricsModel.DB_TABLE}
         | WHERE ${MetricsModel.Field.COD_YEAR} == ${DateUtil.getYear(date)}
         | AND ${MetricsModel.Field.COD_MONTH} == ${DateUtil.getMonth(date)}""".stripMargin
    )
  }

  def save(spark:SparkSession, dataFrame: DataFrame, date:String, paramView:String): Unit = {
    try {
      if(spark.catalog.tableExists(s"${MetricsModel.DB_TABLE}")){
        dataFrame.createOrReplaceTempView("table_metrics")
        val query = s"""INSERT OVERWRITE TABLE ${MetricsModel.DB_TABLE} 
            | PARTITION(
            | ${MetricsModel.Field.COD_YEAR} = ${DateUtil.getYear(date)},
            | ${MetricsModel.Field.COD_MONTH} = ${DateUtil.getMonth(date)},
            | ${MetricsModel.Field.ID_VIEW_DOCUMENT} = ${paramView},
            | ${MetricsModel.Field.ID_METRIC}
            | )
            | SELECT ${MetricsModel.rows.mkString(",")}
            | FROM table_metrics""".stripMargin
        log.info(s"[*] SaveHive: ${query}")
        spark.sql(query)
      } else {
        dataFrame.select(MetricsModel.Field.Schema.fieldNames.map(i => col(i)).toSeq: _*).write.
        mode("overwrite").
        format("orc").option("compression", "snappy").
        partitionBy(MetricsModel.Field.COD_YEAR, MetricsModel.Field.COD_MONTH, MetricsModel.Field.ID_VIEW_DOCUMENT, MetricsModel.Field.ID_METRIC).
        saveAsTable(MetricsModel.DB_TABLE) 
      }
    } catch {
      case e: Exception => {
        log.error(s"[*] ${e.printStackTrace()}")
      }
    }
  }

}
