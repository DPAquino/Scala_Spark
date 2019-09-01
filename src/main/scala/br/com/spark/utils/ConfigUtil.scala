package br.com.spark.utils

import com.typesafe.config.{ConfigObject, ConfigValue, ConfigFactory, Config}
import scala.collection.JavaConverters._  
import java.util.Map.Entry

object ConfigUtil {

  private val configManager = ConfigFactory.load(this.getClass().getClassLoader())

  def getMap(config: String): Map[String, String] = {
    val list : Iterable[ConfigObject] = configManager.getObjectList(config).asScala
    (
      for {
        item : ConfigObject <- list
        entry : Entry[String, ConfigValue] <- item.entrySet().asScala
        key = entry.getKey
        value = entry.getValue.unwrapped().toString
      } yield (key, value)
    ).toMap
  }
  
  val code_view         = getMap("view.code_view")
  val desc_view         = getMap("view.desc_view")
  val code_grp_metric   = getMap("grp_metric.code_grp_metric")
  val desc_grp_metric   = getMap("grp_metric.desc_grp_metric") 
  val desc_metric       = getMap("metric.desc_metric") 
  
  def getSparkPartitions: Int = {
    configManager.getInt("spark.numrepartitions")
  }

  def getHiveWarehouse: String = {
    configManager.getString("hive.warehouse")
  }

} 
