package br.com.spark

// built-in
// thrid party
import org.slf4j.LoggerFactory
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.log4j.{Level, Logger}
// local
import br.com.spark.utils._
import br.com.spark.service.SalesService

object client_metrics {
  val log = LoggerFactory.getLogger(this.getClass)
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)
  def getSparkSession(): SparkSession = {
    val sparkSession = SparkSession.
        builder().
        config("spark.sql.warehouse.dir", ConfigUtil.getHiveWarehouse).
        enableHiveSupport().
        getOrCreate()
    sparkSession.sparkContext.setCheckpointDir("/tmp/checkpoints/client_metrics")
    sparkSession.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    sparkSession.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    sparkSession
  }
  def main(args: Array[String]): Unit = {
    log.info("[*] Beginning")
    val spark = getSparkSession()
    val paramDate = if (args.size > 0) args(0) else null
    val paramView = if (args.size > 1) args(1) else ""
    val paramMetrics = if (args.size > 2) args(2) else ""
    log.info(s"[*] Parameters ${paramDate}, ${paramView}, ${paramMetrics} ")
    val date = DateUtil.getDate(paramDate)
    try {
      log.info("[*] Applying metrics")
      SalesService.saveMetric(spark, date, paramMetrics, paramView)
      log.info("[*] Finished")
      sys.exit(0)
    } catch {
      case e: Exception => 
        log.error(s"[!] ${e.printStackTrace}")
      sys.exit(1)
    }
  }
}
