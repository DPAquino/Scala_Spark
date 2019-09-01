package br.com.spark.utils

// standard library imports
// related third party imports
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers
// local application/library specific imports

class test_ConfigUtil extends FunSuite {
  test("[ConfigUtil] getSparkPartitions ") {
    val config = ConfigUtil.getSparkPartitions
    println(s"[*] ${config}")
  }
  test("[ConfigUtil] getHiveWarehouse ") {
    val config = ConfigUtil.getHiveWarehouse
    println(s"[*] ${config}")
  }
  test("[ConfigUtil] codigo grupo metrica") {
    println(s"[*] ${ConfigUtil.code_grp_metric("1")}")
  }
  test("[ConfigUtil] desc grupo metrica") {
    println(s"[*] ${ConfigUtil.desc_grp_metric("1")}")
  }
}
