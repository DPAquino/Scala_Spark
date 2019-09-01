package br.com.spark.business.rules

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import br.com.spark.business.SalesMetricClassifier

class QtyStoresUDAF extends UserDefinedAggregateFunction with SalesMetricTrait {

  // Input Data Type Schema
  override def inputSchema: StructType = ClientSchema

  // Intermediate Schema
  override def bufferSchema: StructType = BufferSchema_1

  // Returned Data Type
  override def dataType: DataType = ResultSchema

  // Self-explaining
  override def deterministic: Boolean = true

  // This function is called whenever key changes
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    for (i <- 0 until buffer.size) buffer(i) = 0.0
  }

  // Iterate over each entry of a group
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if(buffer != null) {
      val id_source_sale: Int = input.get(idx_id_source_sale).asInstanceOf[Int]
      val ds_site_format: String = input.get(idx_ds_site_format).asInstanceOf[String]
      val cod_ind_site_ecomm: Int = input.get(idx_cod_ind_site_ecomm).asInstanceOf[Int]
      val cod_delivery: Int = input.get(idx_cod_delivery).asInstanceOf[Int]
      val ind_identified: Int       = input.get(idx_ind_identified).asInstanceOf[Int]
      val ind_registered: Int       = input.get(idx_ind_registered).asInstanceOf[Int]
      val lst_sales_items: Seq[Row] = input.get(idx_lst_sales_items).asInstanceOf[Seq[Row]]
      val cod_bus: Seq[String] = lst_sales_items.filter(i => !i.isNullAt(idx_cod_bus)).map(i => i.get(idx_cod_bus).toString).distinct

      val metric_classifier = new SalesMetricClassifier()
      val metric = metric_classifier.check(id_source_sale, ds_site_format, cod_ind_site_ecomm, cod_delivery, cod_bus, ind_identified, ind_registered)
      //---------Counts------------------
      buffer(0) =  if (metric("total"))       { buffer(0).asInstanceOf[Double] } else { buffer(0).asInstanceOf[Double] }
      buffer(1) =  if (metric("regist"))      { buffer(1).asInstanceOf[Double] } else { buffer(1).asInstanceOf[Double] }
      buffer(2) =  if (metric("ident"))       { buffer(2).asInstanceOf[Double] } else { buffer(2).asInstanceOf[Double] }
      buffer(3) =  if (metric("nident"))      { buffer(3).asInstanceOf[Double] } else { buffer(3).asInstanceOf[Double] }
      buffer(4) =  if (metric("store"))       { buffer(4).asInstanceOf[Double] } else { buffer(4).asInstanceOf[Double] }
      buffer(5) =  if (metric("hyp"))         { buffer(5).asInstanceOf[Double] + 1  } else { buffer(5).asInstanceOf[Double] }
      buffer(6) =  if (metric("sup"))         { buffer(6).asInstanceOf[Double] + 1  } else { buffer(6).asInstanceOf[Double] }
      buffer(7) =  if (metric("prx"))         { buffer(7).asInstanceOf[Double] + 1  } else { buffer(7).asInstanceOf[Double] }
      buffer(8) =  if (metric("drg"))         { buffer(8).asInstanceOf[Double] + 1  } else { buffer(8).asInstanceOf[Double] }
      buffer(9) =  if (metric("gas"))         { buffer(9).asInstanceOf[Double] + 1  } else { buffer(9).asInstanceOf[Double] }
      buffer(10) =  if (metric("store_other")){ buffer(10).asInstanceOf[Double] } else { buffer(10).asInstanceOf[Double] }
      buffer(11) =  if (metric("ecm"))        { buffer(11).asInstanceOf[Double] } else { buffer(11).asInstanceOf[Double] }
      buffer(12) =  if (metric("app"))        { buffer(12).asInstanceOf[Double] } else { buffer(12).asInstanceOf[Double] }
      buffer(13) =  if (metric("ecm_std"))    { buffer(13).asInstanceOf[Double] } else { buffer(13).asInstanceOf[Double] }
      buffer(14) =  if (metric("click"))      { buffer(14).asInstanceOf[Double] } else { buffer(14).asInstanceOf[Double] }
      buffer(15) =  if (metric("drive"))      { buffer(15).asInstanceOf[Double] } else { buffer(15).asInstanceOf[Double] }
      buffer(16) =  if (metric("scan"))       { buffer(16).asInstanceOf[Double] } else { buffer(16).asInstanceOf[Double] }
      buffer(17) =  if (metric("rappi"))      { buffer(17).asInstanceOf[Double] } else { buffer(17).asInstanceOf[Double] }
      buffer(18) =  if (metric("ecm_other"))  { buffer(18).asInstanceOf[Double] } else { buffer(18).asInstanceOf[Double] }
      buffer(19) =  if (metric("food"))       { buffer(19).asInstanceOf[Double] + 1 } else { buffer(19).asInstanceOf[Double] }
      buffer(20) =  if (metric("nfood"))      { buffer(20).asInstanceOf[Double] + 1 } else { buffer(20).asInstanceOf[Double] }
    }
  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    if (buffer1 != null && buffer2 != null) {
      for (i <- 0 until buffer1.size) buffer1(i) = buffer1(i).asInstanceOf[Double] + buffer2(i).asInstanceOf[Double]
    }
  }

  // Called after all the entries are exhausted.
  override def evaluate(buffer: Row): Any = {
    var qty = 0.0

    for (i <- 5 to 9) {
      if (buffer(i).asInstanceOf[Double] > 0.0)
        qty += 1
    }

    Seq(
        qty
        , 0.0
        , 0.0
        , 0.0
        , 0.0
        , 0.0
        , 0.0
        , 0.0
        , 0.0
        , 0.0
        , 0.0
        , 0.0
        , 0.0
        , 0.0
        , 0.0
        , 0.0
        , 0.0
        , 0.0
        , 0.0
        , if (buffer(19).asInstanceOf[Double].isNaN) {0.0} else { buffer(19).asInstanceOf[Double] }
        , if (buffer(20).asInstanceOf[Double].isNaN) {0.0} else { buffer(20).asInstanceOf[Double] }
    )
  }
}

