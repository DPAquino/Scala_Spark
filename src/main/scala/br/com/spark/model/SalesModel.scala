package br.com.spark.model

import org.apache.spark.sql.types._

object SalesModel {
  val DB = "db_dolphin_target"
  val TABLE = "tbl_sales_analytics_store"
  val DB_TABLE = s"${DB}.${TABLE}"

  object CouponField {
    val NUM_ISSUE_DATE = "num_issue_date"
    val NUM_CONSUMER_DOCUMENT = "num_consumer_document"
    val NUM_BARCODE = "num_barcode"
    val NM_SOURCE_TRANSACTION = "nm_source_transaction"
    val NUM_REWARD_LINE = "num_reward_line"
    val NUM_BARCODE_COUPON = "num_barcode_coupon"
    val COD_REWARD_TYPE = "cod_reward_type"
    val COD_OMNIA = "cod_omnia"
    val NM_PROMOTION = "nm_promotion"
    val VAL_DISCOUNT_TOTAL = "val_discount_total"
    val TYP_EARNED = "typ_earned"
    val QTD_REWARD = "qtd_reward"
    val DTH_CREATE = "dth_create"

    val Schema: StructType = {
      StructType(
        StructField(NUM_ISSUE_DATE, IntegerType)
        :: StructField(NUM_CONSUMER_DOCUMENT, DecimalType(10,0))
        :: StructField(NUM_BARCODE, DecimalType(38,10))
        :: StructField(NM_SOURCE_TRANSACTION, StringType)
        :: StructField(NUM_REWARD_LINE, DecimalType(38,10))
        :: StructField(NUM_BARCODE_COUPON, StringType)
        :: StructField(COD_REWARD_TYPE, StringType)
        :: StructField(COD_OMNIA, DecimalType(38,10))
        :: StructField(NM_PROMOTION, StringType)
        :: StructField(VAL_DISCOUNT_TOTAL, DecimalType(38,10))
        :: StructField(TYP_EARNED, StringType)
        :: StructField(QTD_REWARD, DecimalType(38,10))
        :: StructField(DTH_CREATE, TimestampType)
        :: Nil        
      )
    }
  }

  object ItemField {
    val NUM_ISSUE_DATE = "num_issue_date"
    val NUM_DOCUMENT = "num_document"
    val NUM_SERIES = "num_series"
    val INI_SITE = "ini_site"
    val COD_PRODUCT_RMS = "cod_product_rms"
    val NUM_ITEM = "num_item"
    val NUM_BARCODE = "num_barcode"
    val DS_PRODUCT = "ds_product"
    val DS_PRODUCT_SHORT = "ds_product_short"
    val COD_CFOP = "cod_cfop"
    val TYP_PRODUCT_UNIT = "typ_product_unit"
    val QTY_ITEM = "qty_item"
    val VAL_UNIT_PRICE = "val_unit_price"
    val VAL_TOTAL = "val_total"
    val COD_GLOBAL_TRADE_ITEM_NUMBER = "cod_global_trade_item_number"
    val VAL_DISCOUNT = "val_discount"
    val VAL_TOTAL_WITH_DISCOUNT = "val_total_with_discount"
    val VAL_TAX = "val_tax"
    val NUM_POS = "num_pos"
    val COD_BUS = "cod_bus"
    val DS_BUS = "ds_bus"
    val COD_MAIN_SUPPLIER = "cod_main_supplier"
    val IND_SALE_STOP = "ind_sale_stop"
    val DS_BRAND_TYP = "ds_brand_typ"
    val DS_BRAND = "ds_brand"
    val COD_SUB_CATEGORY = "cod_sub_category"
    val DS_SUB_CATEGORY = "ds_sub_category"
    val COD_CATEGORY = "cod_category"
    val DS_CATEGORY = "ds_category"
    val COD_GROUP = "cod_group"
    val DS_GROUP = "ds_group"
    val COD_SECTOR = "cod_sector"
    val DS_SECTOR = "ds_sector"
    val COD_DEPARTAMENT = "cod_departament"
    val DS_DEPARTMENT = "ds_departament"
    val DS_PRODUCT_BRAND_TYPE = "ds_product_brand_type"
    val COD_PRODUCT_BRAND_TYPE = "cod_product_brand_type"
    val DS_SUPPLIER = "ds_supplier"
    val DS_SUPPLIER_KEY = "ds_supplier_key"
    
    val Schema: StructType = {
      StructType(
        StructField(NUM_ISSUE_DATE, IntegerType)
        :: StructField(NUM_DOCUMENT, DecimalType(38,10))
        :: StructField(NUM_SERIES, DecimalType(38,10))
        :: StructField(INI_SITE, StringType)
        :: StructField(COD_PRODUCT_RMS, DecimalType(38,10))
        :: StructField(NUM_ITEM, DecimalType(38,10))
        :: StructField(NUM_BARCODE, DecimalType(38,10))
        :: StructField(DS_PRODUCT, StringType)
        :: StructField(DS_PRODUCT_SHORT, StringType)
        :: StructField(COD_CFOP, DecimalType(38,10))
        :: StructField(TYP_PRODUCT_UNIT, StringType)
        :: StructField(QTY_ITEM, DecimalType(38,10))
        :: StructField(VAL_UNIT_PRICE, DecimalType(38,10))
        :: StructField(VAL_TOTAL, DecimalType(38,10))
        :: StructField(COD_GLOBAL_TRADE_ITEM_NUMBER, DecimalType(38,10))
        :: StructField(VAL_DISCOUNT, DecimalType(38,10))
        :: StructField(VAL_TOTAL_WITH_DISCOUNT, DecimalType(38,10))
        :: StructField(VAL_TAX, DecimalType(38,10))
        :: StructField(NUM_POS, DecimalType(38,10))
        :: StructField(COD_BUS, StringType)
        :: StructField(DS_BUS, StringType)
        :: StructField(COD_MAIN_SUPPLIER, DecimalType(38,18))
        :: StructField(IND_SALE_STOP, DecimalType(38,18))
        :: StructField(DS_BRAND_TYP, StringType)
        :: StructField(DS_BRAND, StringType)
        :: StructField(COD_SUB_CATEGORY, DecimalType(38,18))
        :: StructField(DS_SUB_CATEGORY, StringType)
        :: StructField(COD_CATEGORY, DecimalType(38,18))
        :: StructField(DS_CATEGORY, StringType)
        :: StructField(COD_GROUP, DecimalType(38,18))
        :: StructField(DS_GROUP, StringType)
        :: StructField(COD_SECTOR, DecimalType(38,18))
        :: StructField(DS_SECTOR, StringType)
        :: StructField(COD_DEPARTAMENT, DecimalType(38,18))
        :: StructField(DS_DEPARTMENT, StringType)
        :: StructField(DS_PRODUCT_BRAND_TYPE, StringType)
        :: StructField(COD_PRODUCT_BRAND_TYPE, DecimalType(38,18))
        :: StructField(DS_SUPPLIER, StringType)
        :: StructField(DS_SUPPLIER_KEY, StringType)
        :: Nil        
      )
    }
  }

  object Field {
    val ID_SOURCE_SALE = "id_source_sale"
    val DS_SOURCE_SALE = "ds_source_sale"
    val ID_SALE = "id_sale"
    val DTH_ISSUE = "dth_issue"
    val NUM_ISSUE_DATE = "num_issue_date"
    val NUM_ISSUER_CNPJ = "num_issuer_cnpj"
    val NUM_DOCUMENT = "num_document"
    val NUM_SERIES = "num_series"
    val COD_SITE = "cod_site"
    val DS_SITE_FORMAT = "ds_site_format"
    val COD_UF = "cod_uf"
    val DS_OPERATION_NATURE = "ds_operation_nature"
    val NUM_POS = "num_pos"
    val DS_ISSUER_TRADE_NAME = "ds_issuer_trade_name"
    val DS_ISSUER_CITY = "ds_issuer_city"
    val INI_ISSUER_UF = "ini_issuer_uf"
    val NUM_ISSUER_ZIP_CODE = "num_issuer_zip_code"
    val COD_CONSUMER_CITY = "cod_consumer_city"
    val DS_CONSUMER_CITY = "ds_consumer_city"
    val COD_CONSUMER_STATE = "cod_consumer_state"
    val COD_CONSUMER_ZIP_CODE = "cod_consumer_zip_code"
    val DS_CONSUMER_EMAIL = "ds_consumer_email"
    val VAL_PRODUCT_TOTAL = "val_product_total"
    val VAL_FREIGHT_TOTAL = "val_freight_total"
    val VAL_INSURANCE_TOTAL = "val_insurance_total"
    val VAL_DISCOUNT_TOTAL = "val_discount_total"
    val VAL_DOCUMENT_TOTAL = "val_document_total"
    val VAL_TAX_TOTAL = "val_tax_total"
    val NUM_POS_TRANSACTION = "num_pos_transaction"
    val DT_SALE_INITIAL_POS = "dt_sale_initial_pos"
    val HH_SALE_INITIAL_POS = "hh_sale_initial_pos"
    val DT_SALE_FINAL_POS = "dt_sale_final_pos"
    val HH_SALE_FINAL_POS = "hh_sale_final_pos"
    val NUM_CONSUMER_DOCUMENT_NF = "num_consumer_document_nf"
    val NUM_CONSUMER_DOCUMENT_CRM = "num_consumer_document_crm"
    val NUM_CONSUMER_DOCUMENT_PRIORITY = "num_consumer_document_priority"
    val TYP_CONSUMER_DOCUMENT_PRIORITY = "typ_consumer_document_priority"
    val VAL_SERVICES_TOTAL = "val_services_total"
    val VAL_DOCUMENT_TOTAL_WITH_SERVICES = "val_document_total_with_services"
    val LST_COD_BIN = "lst_cod_bin"
    val LST_COUPON_PRINTED = "lst_coupon_printed"
    val LST_COUPON_BURNT = "lst_coupon_burnt"
    val LST_SALES_ITEMS = "lst_sales_items"
    val IND_RAPPI_BIN = "ind_rappi_bin"
    val IND_RAPPI_COUPON = "ind_rappi_coupon"
    val IND_IDENTIFIED = "ind_identified"
    val IND_REGISTERED = "ind_registered"
    val QTY_TOTAL_ITEM = "qty_total_item"
    val QTY_TOTAL_COUPON_PRINTED = "qty_total_coupon_printed"
    val QTY_TOTAL_COUPON_BURNT = "qty_total_coupon_burnt"
    val COD_IND_SITE_ECOMM = "cod_ind_site_ecomm"
    val COD_BUS = "cod_bus"
    val COD_IND_SALES_ECOMM = "cod_ind_sales_ecomm"
    val DS_IND_SALES_ECOMM = "ds_ind_sales_ecomm"
    val DS_BUS = "ds_bus"
    val COD_DELIVERY = "cod_delivery"
    val DS_SHORT_DELIVERY = "ds_short_delivery"
    val DS_SUB_DELIVERY_MACRO = "ds_sub_delivery_macro"
    val TYP_DELIVERY_CATEGORY = "typ_delivery_category"
    val DS_DELIVERY = "ds_delivery"
    val DTH_INGESTION = "dth_ingestion"
    val JOBDATE_SPLIT = "jobdate_split"
  
    val Schema: StructType = {
      StructType(
        StructField(Field.ID_SOURCE_SALE, IntegerType)
        :: StructField(Field.DS_SOURCE_SALE, StringType)
        :: StructField(Field.ID_SALE, StringType)
        :: StructField(Field.DTH_ISSUE, TimestampType)
        :: StructField(Field.NUM_ISSUE_DATE, IntegerType)
        :: StructField(Field.NUM_ISSUER_CNPJ, DecimalType(38,10))
        :: StructField(Field.NUM_DOCUMENT, DecimalType(38,10))
        :: StructField(Field.NUM_SERIES, DecimalType(38,10))
        :: StructField(Field.COD_SITE, DecimalType(38,10))
        :: StructField(Field.DS_SITE_FORMAT, StringType)
        :: StructField(Field.COD_UF, DecimalType(38,10))
        :: StructField(Field.DS_OPERATION_NATURE, StringType)
        :: StructField(Field.NUM_POS, DecimalType(38,10))
        :: StructField(Field.DS_ISSUER_TRADE_NAME, StringType)
        :: StructField(Field.DS_ISSUER_CITY, StringType)
        :: StructField(Field.INI_ISSUER_UF, StringType)
        :: StructField(Field.NUM_ISSUER_ZIP_CODE, DecimalType(38,10))
        :: StructField(Field.COD_CONSUMER_CITY, DecimalType(38,10))
        :: StructField(Field.DS_CONSUMER_CITY, StringType)
        :: StructField(Field.COD_CONSUMER_STATE, StringType)
        :: StructField(Field.COD_CONSUMER_ZIP_CODE, DecimalType(38,10))
        :: StructField(Field.DS_CONSUMER_EMAIL, StringType)
        :: StructField(Field.VAL_PRODUCT_TOTAL, DecimalType(38,10))
        :: StructField(Field.VAL_FREIGHT_TOTAL, DecimalType(38,10))
        :: StructField(Field.VAL_INSURANCE_TOTAL, DecimalType(38,10))
        :: StructField(Field.VAL_DISCOUNT_TOTAL, DecimalType(38,10))
        :: StructField(Field.VAL_DOCUMENT_TOTAL, DecimalType(38,10))
        :: StructField(Field.VAL_TAX_TOTAL, DecimalType(38,10))
        :: StructField(Field.NUM_POS_TRANSACTION, DecimalType(38,10))
        :: StructField(Field.DT_SALE_INITIAL_POS, TimestampType)
        :: StructField(Field.HH_SALE_INITIAL_POS, TimestampType)
        :: StructField(Field.DT_SALE_FINAL_POS, TimestampType)
        :: StructField(Field.HH_SALE_FINAL_POS, TimestampType)
        :: StructField(Field.NUM_CONSUMER_DOCUMENT_NF, DecimalType(38,10))
        :: StructField(Field.NUM_CONSUMER_DOCUMENT_CRM, DecimalType(38,10))
        :: StructField(Field.NUM_CONSUMER_DOCUMENT_PRIORITY, DecimalType(38,10))
        :: StructField(Field.TYP_CONSUMER_DOCUMENT_PRIORITY, StringType)
        :: StructField(Field.VAL_SERVICES_TOTAL, DecimalType(38,10))
        :: StructField(Field.VAL_DOCUMENT_TOTAL_WITH_SERVICES, DecimalType(38,10))
        :: StructField(Field.LST_COD_BIN, StringType)
        :: StructField(Field.LST_COUPON_PRINTED, ArrayType(CouponField.Schema))
        :: StructField(Field.LST_COUPON_BURNT, ArrayType(CouponField.Schema))
        :: StructField(Field.LST_SALES_ITEMS, ArrayType(ItemField.Schema))
        :: StructField(Field.IND_RAPPI_BIN, IntegerType)
        :: StructField(Field.IND_RAPPI_COUPON, IntegerType)
        :: StructField(Field.IND_IDENTIFIED, IntegerType)
        :: StructField(Field.IND_REGISTERED, IntegerType)
        :: StructField(Field.QTY_TOTAL_ITEM, DecimalType(38,10))
        :: StructField(Field.QTY_TOTAL_COUPON_PRINTED, DecimalType(38,10))
        :: StructField(Field.QTY_TOTAL_COUPON_BURNT, DecimalType(38,10))
        :: StructField(Field.COD_IND_SITE_ECOMM, IntegerType)
        :: StructField(Field.COD_BUS, StringType)
        :: StructField(Field.COD_IND_SALES_ECOMM, IntegerType)
        :: StructField(Field.DS_IND_SALES_ECOMM, StringType)
        :: StructField(Field.DS_BUS, StringType)
        :: StructField(Field.COD_DELIVERY, IntegerType)
        :: StructField(Field.DS_SHORT_DELIVERY, StringType)
        :: StructField(Field.DS_SUB_DELIVERY_MACRO, StringType)
        :: StructField(Field.TYP_DELIVERY_CATEGORY, StringType)
        :: StructField(Field.DS_DELIVERY, StringType)
        :: StructField(Field.DTH_INGESTION, TimestampType)
        :: StructField(Field.JOBDATE_SPLIT, IntegerType)
        :: Nil
      )
    }
  }
  
  val rows = Seq(
    Field.ID_SOURCE_SALE
    , Field.DS_SOURCE_SALE 
    , Field.ID_SALE 
    , Field.COD_SITE 
    , Field.DS_SITE_FORMAT 
    , Field.COD_UF 
    , Field.NUM_CONSUMER_DOCUMENT_PRIORITY 
    , Field.VAL_DOCUMENT_TOTAL_WITH_SERVICES 
    , Field.LST_SALES_ITEMS 
    , Field.LST_COUPON_BURNT 
    , Field.COD_IND_SITE_ECOMM 
    , Field.COD_BUS 
    , Field.COD_DELIVERY 
    , Field.QTY_TOTAL_COUPON_BURNT
    , Field.IND_IDENTIFIED
    , Field.IND_REGISTERED
  )

}
