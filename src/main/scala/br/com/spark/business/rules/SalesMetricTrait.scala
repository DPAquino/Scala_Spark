package br.com.spark.business.rules

import org.apache.spark.sql.{Row}
import org.apache.spark.sql.types._
import br.com.spark.model.UdafModel

trait SalesMetricTrait {
  val ClientSchema = UdafModel.ClientSchema
  val ResultSchema = UdafModel.ResultSchema
  val ItemClientSchema = UdafModel.ItemClientSchema
 
  val idx_id_source_sale = ClientSchema.fieldIndex(UdafModel.ID_SOURCE_SALE)
  val idx_ds_site_format = ClientSchema.fieldIndex(UdafModel.DS_SITE_FORMAT)
  val idx_cod_ind_site_ecomm = ClientSchema.fieldIndex(UdafModel.COD_IND_SITE_ECOMM)
  val idx_lst_sales_items = ClientSchema.fieldIndex(UdafModel.LST_SALES_ITEMS)
  val idx_cod_delivery = ClientSchema.fieldIndex(UdafModel.COD_DELIVERY)
  val idx_cod_bus = ItemClientSchema.fieldIndex(UdafModel.COD_BUS)
  val idx_cod_product_rms = ItemClientSchema.fieldIndex(UdafModel.COD_PRODUCT_RMS)
  val idx_val_document_total_with_services = ClientSchema.fieldIndex(UdafModel.VAL_DOCUMENT_TOTAL_WITH_SERVICES)
  val idx_val_unit_price = ItemClientSchema.fieldIndex(UdafModel.VAL_UNIT_PRICE)
  val idx_val_total_with_discount = ItemClientSchema.fieldIndex(UdafModel.VAL_TOTAL_WITH_DISCOUNT)
  val idx_ds_sector = ItemClientSchema.fieldIndex(UdafModel.DS_SECTOR)
  val idx_ds_department = ItemClientSchema.fieldIndex(UdafModel.DS_DEPARTMENT)
  val idx_ds_group = ItemClientSchema.fieldIndex(UdafModel.DS_GROUP)
  val idx_qty_total_coupon_burnt = ClientSchema.fieldIndex(UdafModel.QTY_TOTAL_COUPON_BURNT)
  val idx_qty_item = ItemClientSchema.fieldIndex(UdafModel.QTY_ITEM)
  val idx_ind_identified = ClientSchema.fieldIndex(UdafModel.IND_IDENTIFIED)
  val idx_ind_registered = ClientSchema.fieldIndex(UdafModel.IND_REGISTERED)
  val idx_num_consumer_document_priority = ClientSchema.fieldIndex(UdafModel.NUM_CONSUMER_DOCUMENT_PRIORITY)

  val BufferSchema_1: StructType = 
    StructType(
      StructField("val_total", DoubleType)
      :: StructField("val_regist", DoubleType)
      :: StructField("val_ident", DoubleType)
      :: StructField("val_nident", DoubleType)
      :: StructField("val_store", DoubleType)
      :: StructField("val_hyp", DoubleType)
      :: StructField("val_sup", DoubleType)
      :: StructField("val_prx", DoubleType)
      :: StructField("val_drg", DoubleType)
      :: StructField("val_gas", DoubleType)
      :: StructField("val_store_other", DoubleType)
      :: StructField("val_ecm", DoubleType)
      :: StructField("val_app", DoubleType)
      :: StructField("val_ecm_std", DoubleType)
      :: StructField("val_click", DoubleType)
      :: StructField("val_drive", DoubleType)
      :: StructField("val_scan", DoubleType)
      :: StructField("val_rappi", DoubleType)
      :: StructField("val_ecm_other", DoubleType)
      :: StructField("val_food", DoubleType)
      :: StructField("val_nfood", DoubleType)
      :: Nil
    )
  
  def EvaluateBuffer_1(buffer:Row): Seq[Double] = {
    Seq(
      if (buffer(0).asInstanceOf[Double].isNaN) {0.0} else { buffer(0).asInstanceOf[Double] }
      , if (buffer(1).asInstanceOf[Double].isNaN) {0.0} else { buffer(1).asInstanceOf[Double] }
      , if (buffer(2).asInstanceOf[Double].isNaN) {0.0} else { buffer(2).asInstanceOf[Double] }
      , if (buffer(3).asInstanceOf[Double].isNaN) {0.0} else { buffer(3).asInstanceOf[Double] }
      , if (buffer(4).asInstanceOf[Double].isNaN) {0.0} else { buffer(4).asInstanceOf[Double] }
      , if (buffer(5).asInstanceOf[Double].isNaN) {0.0} else { buffer(5).asInstanceOf[Double] }
      , if (buffer(6).asInstanceOf[Double].isNaN) {0.0} else { buffer(6).asInstanceOf[Double] }
      , if (buffer(7).asInstanceOf[Double].isNaN) {0.0} else { buffer(7).asInstanceOf[Double] }
      , if (buffer(8).asInstanceOf[Double].isNaN) {0.0} else { buffer(8).asInstanceOf[Double] }
      , if (buffer(9).asInstanceOf[Double].isNaN) {0.0} else { buffer(9).asInstanceOf[Double] }
      , if (buffer(10).asInstanceOf[Double].isNaN) {0.0} else { buffer(10).asInstanceOf[Double] }
      , if (buffer(11).asInstanceOf[Double].isNaN) {0.0} else { buffer(11).asInstanceOf[Double] }
      , if (buffer(12).asInstanceOf[Double].isNaN) {0.0} else { buffer(12).asInstanceOf[Double] }
      , if (buffer(13).asInstanceOf[Double].isNaN) {0.0} else { buffer(13).asInstanceOf[Double] }
      , if (buffer(14).asInstanceOf[Double].isNaN) {0.0} else { buffer(14).asInstanceOf[Double] }
      , if (buffer(15).asInstanceOf[Double].isNaN) {0.0} else { buffer(15).asInstanceOf[Double] }
      , if (buffer(16).asInstanceOf[Double].isNaN) {0.0} else { buffer(16).asInstanceOf[Double] }
      , if (buffer(17).asInstanceOf[Double].isNaN) {0.0} else { buffer(17).asInstanceOf[Double] }
      , if (buffer(18).asInstanceOf[Double].isNaN) {0.0} else { buffer(18).asInstanceOf[Double] }
      , if (buffer(19).asInstanceOf[Double].isNaN) {0.0} else { buffer(19).asInstanceOf[Double] }
      , if (buffer(20).asInstanceOf[Double].isNaN) {0.0} else { buffer(20).asInstanceOf[Double] }
    )
  }
  
  val BufferSchema_2: StructType = 
    StructType(
      StructField("qty_total", DoubleType)
      :: StructField("qty_regist", DoubleType)
      :: StructField("qty_ident", DoubleType)
      :: StructField("qty_nident", DoubleType)
      :: StructField("qty_store", DoubleType)
      :: StructField("qty_hyp", DoubleType)
      :: StructField("qty_sup", DoubleType)
      :: StructField("qty_prx", DoubleType)
      :: StructField("qty_drg", DoubleType)
      :: StructField("qty_gas", DoubleType)
      :: StructField("qty_store_other", DoubleType)
      :: StructField("qty_ecm", DoubleType)
      :: StructField("qty_app", DoubleType)
      :: StructField("qty_ecm_std", DoubleType)
      :: StructField("qty_click", DoubleType)
      :: StructField("qty_drive", DoubleType)
      :: StructField("qty_scan", DoubleType)
      :: StructField("qty_rappi", DoubleType)
      :: StructField("qty_ecm_other", DoubleType)
      :: StructField("qty_food", DoubleType)
      :: StructField("qty_nfood", DoubleType)
      :: StructField("val_total", DoubleType)
      :: StructField("val_regist", DoubleType)
      :: StructField("val_ident", DoubleType)
      :: StructField("val_nident", DoubleType)
      :: StructField("val_store", DoubleType)
      :: StructField("val_hyp", DoubleType)
      :: StructField("val_sup", DoubleType)
      :: StructField("val_prx", DoubleType)
      :: StructField("val_drg", DoubleType)
      :: StructField("val_gas", DoubleType)
      :: StructField("val_store_other", DoubleType)
      :: StructField("val_ecm", DoubleType)
      :: StructField("val_app", DoubleType)
      :: StructField("val_ecm_std", DoubleType)
      :: StructField("val_click", DoubleType)
      :: StructField("val_drive", DoubleType)
      :: StructField("val_scan", DoubleType)
      :: StructField("val_rappi", DoubleType)
      :: StructField("val_ecm_other", DoubleType)
      :: StructField("val_food", DoubleType)
      :: StructField("val_nfood", DoubleType)
      :: Nil
    )
  
  def EvaluateBuffer_2(buffer:Row): Seq[Double] = {
    val ticket_total       = buffer(21).asInstanceOf[Double] / buffer(0).asInstanceOf[Double]
    val ticket_regist      = buffer(22).asInstanceOf[Double] / buffer(1).asInstanceOf[Double]
    val ticket_ident       = buffer(23).asInstanceOf[Double] / buffer(2).asInstanceOf[Double]
    val ticket_nident      = buffer(24).asInstanceOf[Double] / buffer(3).asInstanceOf[Double]
    val ticket_store       = buffer(25).asInstanceOf[Double] / buffer(4).asInstanceOf[Double]
    val ticket_hyp         = buffer(26).asInstanceOf[Double] / buffer(5).asInstanceOf[Double]
    val ticket_sup         = buffer(27).asInstanceOf[Double] / buffer(6).asInstanceOf[Double]
    val ticket_prx         = buffer(28).asInstanceOf[Double] / buffer(7).asInstanceOf[Double]
    val ticket_drg         = buffer(29).asInstanceOf[Double] / buffer(8).asInstanceOf[Double]
    val ticket_gas         = buffer(30).asInstanceOf[Double] / buffer(9).asInstanceOf[Double]
    val ticket_store_other = buffer(31).asInstanceOf[Double] / buffer(10).asInstanceOf[Double]
    val ticket_ecm         = buffer(32).asInstanceOf[Double] / buffer(11).asInstanceOf[Double]
    val ticket_app         = buffer(33).asInstanceOf[Double] / buffer(12).asInstanceOf[Double]
    val ticket_ecm_std     = buffer(34).asInstanceOf[Double] / buffer(13).asInstanceOf[Double]
    val ticket_click       = buffer(35).asInstanceOf[Double] / buffer(14).asInstanceOf[Double]
    val ticket_drive       = buffer(36).asInstanceOf[Double] / buffer(15).asInstanceOf[Double]
    val ticket_scan        = buffer(37).asInstanceOf[Double] / buffer(16).asInstanceOf[Double]
    val ticket_rappi       = buffer(38).asInstanceOf[Double] / buffer(17).asInstanceOf[Double]
    val ticket_ecm_other   = buffer(39).asInstanceOf[Double] / buffer(18).asInstanceOf[Double]
    val ticket_food        = buffer(40).asInstanceOf[Double] / buffer(19).asInstanceOf[Double]
    val ticket_nfood       = buffer(41).asInstanceOf[Double] / buffer(20).asInstanceOf[Double]
    Seq(
      if (ticket_total.isNaN)          { 0.0 } else { ticket_total }
      , if (ticket_regist.isNaN)       { 0.0 } else { ticket_regist }
      , if (ticket_ident.isNaN)        { 0.0 } else { ticket_ident }
      , if (ticket_nident.isNaN)       { 0.0 } else { ticket_nident }
      , if (ticket_store.isNaN)        { 0.0 } else { ticket_store }
      , if (ticket_hyp.isNaN)          { 0.0 } else { ticket_hyp }
      , if (ticket_sup.isNaN)          { 0.0 } else { ticket_sup }
      , if (ticket_prx.isNaN)          { 0.0 } else { ticket_prx }
      , if (ticket_drg.isNaN)          { 0.0 } else { ticket_drg }
      , if (ticket_gas.isNaN)          { 0.0 } else { ticket_gas }
      , if (ticket_store_other.isNaN)  { 0.0 } else { ticket_store_other }
      , if (ticket_ecm.isNaN)          { 0.0 } else { ticket_ecm }
      , if (ticket_app.isNaN)          { 0.0 } else { ticket_app }
      , if (ticket_ecm_std.isNaN)      { 0.0 } else { ticket_ecm_std }
      , if (ticket_click.isNaN)        { 0.0 } else { ticket_click }
      , if (ticket_drive.isNaN)        { 0.0 } else { ticket_drive }
      , if (ticket_scan.isNaN)         { 0.0 } else { ticket_scan }
      , if (ticket_rappi.isNaN)        { 0.0 } else { ticket_rappi }
      , if (ticket_ecm_other.isNaN)    { 0.0 } else { ticket_ecm_other }
      , if (ticket_food.isNaN)         { 0.0 } else { ticket_food }
      , if (ticket_nfood.isNaN)        { 0.0 } else { ticket_nfood }
    )
  }
  
  val BufferSchema_3: StructType = 
    StructType(
      StructField("val_total", MapType(StringType, IntegerType))
      :: StructField("val_regist", MapType(StringType, IntegerType))
      :: StructField("val_ident", MapType(StringType, IntegerType))
      :: StructField("val_nident", MapType(StringType, IntegerType))
      :: StructField("val_store", MapType(StringType, IntegerType))
      :: StructField("val_hyp", MapType(StringType, IntegerType))
      :: StructField("val_sup", MapType(StringType, IntegerType))
      :: StructField("val_prx", MapType(StringType, IntegerType))
      :: StructField("val_drg", MapType(StringType, IntegerType))
      :: StructField("val_gas", MapType(StringType, IntegerType))
      :: StructField("val_store_other", MapType(StringType, IntegerType))
      :: StructField("val_ecm", MapType(StringType, IntegerType))
      :: StructField("val_app", MapType(StringType, IntegerType))
      :: StructField("val_ecm_std", MapType(StringType, IntegerType))
      :: StructField("val_click", MapType(StringType, IntegerType))
      :: StructField("val_drive", MapType(StringType, IntegerType))
      :: StructField("val_scan", MapType(StringType, IntegerType))
      :: StructField("val_rappi", MapType(StringType, IntegerType))
      :: StructField("val_ecm_other", MapType(StringType, IntegerType))
      :: StructField("val_food", MapType(StringType, IntegerType))
      :: StructField("val_nfood", MapType(StringType, IntegerType))
      :: Nil
    )

  def EvaluateBuffer_3(buffer:Row): Seq[Double] = {
    Seq(
        if (buffer(0).asInstanceOf[Map[String, Int]].keySet.isEmpty)   {0.0} else { buffer(0).asInstanceOf[Map[String, Int]].keySet.size }
        , if (buffer(1).asInstanceOf[Map[String, Int]].keySet.isEmpty) {0.0} else { buffer(1).asInstanceOf[Map[String, Int]].keySet.size }
        , if (buffer(2).asInstanceOf[Map[String, Int]].keySet.isEmpty) {0.0} else { buffer(2).asInstanceOf[Map[String, Int]].keySet.size }
        , if (buffer(3).asInstanceOf[Map[String, Int]].keySet.isEmpty) {0.0} else { buffer(3).asInstanceOf[Map[String, Int]].keySet.size }
        , if (buffer(4).asInstanceOf[Map[String, Int]].keySet.isEmpty) {0.0} else { buffer(4).asInstanceOf[Map[String, Int]].keySet.size }
        , if (buffer(5).asInstanceOf[Map[String, Int]].keySet.isEmpty) {0.0} else { buffer(5).asInstanceOf[Map[String, Int]].keySet.size }
        , if (buffer(6).asInstanceOf[Map[String, Int]].keySet.isEmpty) {0.0} else { buffer(6).asInstanceOf[Map[String, Int]].keySet.size }
        , if (buffer(7).asInstanceOf[Map[String, Int]].keySet.isEmpty) {0.0} else { buffer(7).asInstanceOf[Map[String, Int]].keySet.size }
        , if (buffer(8).asInstanceOf[Map[String, Int]].keySet.isEmpty) {0.0} else { buffer(8).asInstanceOf[Map[String, Int]].keySet.size }
        , if (buffer(9).asInstanceOf[Map[String, Int]].keySet.isEmpty) {0.0} else { buffer(9).asInstanceOf[Map[String, Int]].keySet.size }
        , if (buffer(10).asInstanceOf[Map[String, Int]].keySet.isEmpty) {0.0} else { buffer(10).asInstanceOf[Map[String, Int]].keySet.size }
        , if (buffer(11).asInstanceOf[Map[String, Int]].keySet.isEmpty) {0.0} else { buffer(11).asInstanceOf[Map[String, Int]].keySet.size }
        , if (buffer(12).asInstanceOf[Map[String, Int]].keySet.isEmpty) {0.0} else { buffer(12).asInstanceOf[Map[String, Int]].keySet.size }
        , if (buffer(13).asInstanceOf[Map[String, Int]].keySet.isEmpty) {0.0} else { buffer(13).asInstanceOf[Map[String, Int]].keySet.size }
        , if (buffer(14).asInstanceOf[Map[String, Int]].keySet.isEmpty) {0.0} else { buffer(14).asInstanceOf[Map[String, Int]].keySet.size }
        , if (buffer(15).asInstanceOf[Map[String, Int]].keySet.isEmpty) {0.0} else { buffer(15).asInstanceOf[Map[String, Int]].keySet.size }
        , if (buffer(16).asInstanceOf[Map[String, Int]].keySet.isEmpty) {0.0} else { buffer(16).asInstanceOf[Map[String, Int]].keySet.size }
        , if (buffer(17).asInstanceOf[Map[String, Int]].keySet.isEmpty) {0.0} else { buffer(17).asInstanceOf[Map[String, Int]].keySet.size }
        , if (buffer(18).asInstanceOf[Map[String, Int]].keySet.isEmpty) {0.0} else { buffer(18).asInstanceOf[Map[String, Int]].keySet.size }
        , if (buffer(19).asInstanceOf[Map[String, Int]].keySet.isEmpty) {0.0} else { buffer(19).asInstanceOf[Map[String, Int]].keySet.size }
        , if (buffer(20).asInstanceOf[Map[String, Int]].keySet.isEmpty) {0.0} else { buffer(20).asInstanceOf[Map[String, Int]].keySet.size }
    )
  }  
}
