package br.com.spark.business

import br.com.spark.utils.ConfigUtil

class SalesMetricClassifier {
  
  def check_service_ecom(
      id_source_sale: Int
      , cod_ind_site_ecomm: Int
      , cod_delivery: Int
      , cod_bus: Seq[String]
  ): Map[String, Boolean] = {
    if (id_source_sale == 2) {
      Map(
        "ecm"         -> true
        , "ecm_std"   -> (cod_ind_site_ecomm == 1 && (cod_bus.contains("NFOOD") && Seq(0,1,2,3).contains(cod_delivery)) || (cod_bus.contains("FOOD") && cod_delivery == 3))
        , "click"     -> (cod_ind_site_ecomm == 1 && cod_bus.contains("NFOOD") && Seq(4,5).contains(cod_delivery))
        , "drive"     -> (cod_ind_site_ecomm == 1 && cod_bus.contains("FOOD") && cod_delivery == 4)
        , "scan"      -> (cod_ind_site_ecomm == 0 && cod_delivery == 6)
        , "rappi"     -> (cod_ind_site_ecomm == 1 && cod_bus.contains("FOOD") && cod_delivery == 7)
        , "ecm_other" -> (((cod_ind_site_ecomm == 0) || ((cod_bus.contains("FOOD") || !Seq(0,1,2,3,4,5,8).contains(cod_delivery)) && (cod_bus.contains("NFOOD") || !Seq(3,4,7).contains(cod_delivery)))) && (cod_ind_site_ecomm == 1 || !Seq(6).contains(cod_delivery)))
      ) 
    } else {
      Map(
        "ecm"         -> false 
        , "ecm_std"   -> false 
        , "click"     -> false 
        , "drive"     -> false 
        , "scan"      -> false 
        , "rappi"     -> false 
        , "ecm_other" -> false 
      ) 
    }
  }

  def check_store(
      id_source_sale: Int
      , ds_site_format: String
  ): Map[String, Boolean] = {
    if (id_source_sale == 1) {
      Map(
        "store"   -> true 
        , "hyp"   -> (ds_site_format.contains("HIPERMERCADO"))  
        , "sup"   -> (ds_site_format.contains("SUPERMERCADO"))  
        , "prx"   -> (ds_site_format.contains("EXPRESS"))       
        , "drg"   -> (ds_site_format.contains("DROGARIA"))      
        , "gas"   -> (ds_site_format.contains("POSTO"))         
        , "store_other" -> !Seq("HIPERMERCADO","SUPERMERCADO","EXPRESS","DROGARIA","POSTO").contains(ds_site_format)
      ) 
    } else {
      Map(
        "store"   -> false 
        , "hyp"   -> false 
        , "sup"   -> false 
        , "prx"   -> false 
        , "drg"   -> false 
        , "gas"   -> false 
        , "store_other" -> false 
      ) 
    }
  }

  def check_food_nfood(cod_bus: Seq[String]): Map[String, Boolean] = {
    Map(
      "food"    -> (cod_bus.contains("FOOD"))
      , "nfood" -> (cod_bus.contains("NFOOD")) 
    )
  }

  def check(
      id_source_sale: Int
      , ds_site_format: String
      , cod_ind_site_ecomm: Int
      , cod_delivery: Int
      , cod_bus: Seq[String]
      , ind_identified: Int = 0
      , ind_registered: Int = 0
  ): Map[String, Boolean] = {
      Map(
        "total" -> true
        , "regist"  -> (ind_registered > 0)
        , "ident"   -> (ind_identified > 0)
        , "nident"  -> (ind_identified == 0)
        , "app"     -> false
      ) ++ 
      check_service_ecom(id_source_sale, cod_ind_site_ecomm, cod_delivery, cod_bus) ++ 
      check_store(id_source_sale, ds_site_format) ++ 
      check_food_nfood(cod_bus)
  }

  def buffer(metric: Map[String, Boolean], val_default:Double, val_food:Double, val_nfood:Double):Array[Double] = {
    val metrics = ConfigUtil.desc_metric.keys.toSeq.map(i => i.toInt).sortWith(_ < _)
    var bufferResult: Array[Double] = new Array(metrics.size)
    for(i <- metrics) {
      bufferResult(i) = 
        if (metric(ConfigUtil.desc_metric(s"${i}"))) { 
          ConfigUtil.desc_metric(s"${i}") match {
            case "food"   => val_food
            case "nfood"  => val_nfood
            case _        => val_default
          } 
        } else { 0.0 }
    }
    bufferResult
  }
  
  def buffer_set(metric: Map[String, Boolean], val_default:Map[String, Int], val_food:Map[String, Int], val_nfood:Map[String, Int]):Array[Map[String, Int]] = {
    val metrics = ConfigUtil.desc_metric.keys.toSeq.map(i => i.toInt).sortWith(_ < _)
    var bufferResult: Array[Map[String, Int]] = new Array(metrics.size)
    for(i <- metrics) {
      bufferResult(i) = 
        if (metric(ConfigUtil.desc_metric(s"${i}"))) { 
          ConfigUtil.desc_metric(s"${i}") match {
            case "food"   => val_food
            case "nfood"  => val_nfood
            case _        => val_default
          } 
        } else { Map() }
    }
    bufferResult
  }

}

