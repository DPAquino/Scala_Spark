package br.com.spark.model

import org.apache.spark.sql.types._

object MetricsModel {
  val DB = "DB_DOLPHIN_TARGET"
  val TABLE = "TBL_CLIENT_METRICS"
  val DB_TABLE = s"${DB}.${TABLE}"

  object Field {
    val NUM_DOCUMENT = "id_source_view"
    val COD_UF = "cod_uf"
    val COD_YEAR = "cod_year"
    val COD_MONTH = "cod_month"
    val ID_METRIC = "id_grp"
    val ID_KPI = "id_segment"
    val VAL_KPI = "val_metric"
    val DTH_INGESTION = "dth_ingestion"
    val DS_KPI = "ds_segment"
    val DS_METRIC = "ds_grp"
    val ID_VIEW_DOCUMENT = "id_view"
    val DS_VIEW_DOCUMENT = "ds_view"

    val Schema: StructType = {
      StructType(
        StructField(DS_VIEW_DOCUMENT, StringType)
          :: StructField(NUM_DOCUMENT, StringType)
          :: StructField(COD_UF, StringType)
          :: StructField(DS_METRIC, StringType)
          :: StructField(ID_KPI, IntegerType)
          :: StructField(DS_KPI, StringType)
          :: StructField(VAL_KPI, DecimalType(38, 10))
          :: StructField(DTH_INGESTION, TimestampType)
          :: StructField(COD_YEAR, IntegerType)
          :: StructField(COD_MONTH, IntegerType)
          :: StructField(ID_VIEW_DOCUMENT, IntegerType)
          :: StructField(ID_METRIC, IntegerType)
          :: Nil
      )
    }
  }

  val rows: Seq[String] = Seq(
    Field.DS_VIEW_DOCUMENT
    , Field.NUM_DOCUMENT   
    , Field.COD_UF
    , Field.DS_METRIC
    , Field.ID_KPI
    , Field.DS_KPI
    , Field.VAL_KPI
    , Field.DTH_INGESTION
    , Field.ID_METRIC
  )
}
