package br.com.spark.dao

// built-in
import org.slf4j.LoggerFactory
// thrid party
import org.apache.spark.sql.{DataFrame, SparkSession}
// local
import br.com.spark.model.SalesModel
import br.com.spark.utils.DateUtil

class SalesDao {

  val log = LoggerFactory.getLogger(this.getClass)

  def getAll(spark:SparkSession): DataFrame = {
    spark.table(SalesModel.DB_TABLE)
  }
  
  def getByDay(spark: SparkSession, date: String):DataFrame = {
    spark.sql(
      s"""SELECT ${SalesModel.rows.mkString(",")} FROM ${SalesModel.DB_TABLE} 
      | WHERE ${SalesModel.Field.JOBDATE_SPLIT} == ${date}""".stripMargin
    )
  }

  def getByMonth(spark: SparkSession, date: String):DataFrame = {
    val query = s"""SELECT ${SalesModel.rows.mkString(",")} FROM ${SalesModel.DB_TABLE} 
      | WHERE ${SalesModel.Field.JOBDATE_SPLIT} >= ${DateUtil.getFirstDay(date)} 
      | AND ${SalesModel.Field.JOBDATE_SPLIT} <= ${DateUtil.getLastDay(date)}
      | """.stripMargin
    log.info(query)
    spark.sql(query)
  }
//| AND num_consumer_document_priority in (10000006726,10000007706,10000086657,10000171760,10000187844,10000199931,10000234605,10000250988,10000714992,10000824402)
  
  def save(dataFrame: DataFrame): Unit = {
    Unit
  }

}
