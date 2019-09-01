package br.com.spark.business.rules

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import br.com.spark.business.SalesMetricClassifier

class QtyDepartmentUDAF extends UserDefinedAggregateFunction with SalesMetricTrait {

  // Input Data Type Schema
  override def inputSchema: StructType = ClientSchema

  // Intermediate Schema
  override def bufferSchema: StructType = BufferSchema_3 

  // Returned Data Type
  override def dataType: DataType = ResultSchema

  // Self-explaining
  override def deterministic: Boolean = true

  // This function is called whenever key changes
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    for (i <- 0 until buffer.size) buffer(i) = Map()
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
      val lst_cod_bus: Map[String, Map[String, Int]] = 
        lst_sales_items.
        filter(i => !i.isNullAt(idx_cod_bus) && !i.isNullAt(idx_ds_department)).
        map(i => (
          i.get(idx_cod_bus).toString
          , Set(i.get(idx_ds_department))
        )).
        groupBy(_._1).
        mapValues(s => s.map(_._2).reduce(_ ++ _).map(i => (i, 1)).toMap.asInstanceOf[Map[String, Int]])
      val amount: Map[String, Int] = if (lst_cod_bus.values.isEmpty) { Map() } else { lst_cod_bus.values.reduce(_ ++ _).asInstanceOf[Map[String, Int]] }
      val metric_classifier = new SalesMetricClassifier()
      val metric = metric_classifier.check(id_source_sale, ds_site_format, cod_ind_site_ecomm, cod_delivery, cod_bus, ind_identified, ind_registered)
      val metric_buffer_val = metric_classifier.buffer_set(metric, amount, lst_cod_bus.getOrElse("FOOD", Map()), lst_cod_bus.getOrElse("NFOOD", Map()))
      for (i <- 0 to buffer.size - 1) {
        buffer(i) = buffer(i).asInstanceOf[Map[String, Int]] ++ metric_buffer_val(i).asInstanceOf[Map[String, Int]]
      }
    }
  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    if (buffer1 != null && buffer2 != null) {
      for (i <- 0 until buffer1.size) buffer1(i) = buffer1(i).asInstanceOf[Map[String, Int]] ++ buffer2(i).asInstanceOf[Map[String, Int]]
    }
  }

  // Called after all the entries are exhausted.
  override def evaluate(buffer: Row): Any = {
    EvaluateBuffer_3(buffer)
  }
}

