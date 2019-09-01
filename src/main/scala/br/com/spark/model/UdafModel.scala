package br.com.spark.model

import org.apache.spark.sql.types._

object UdafModel {
  
  val COD_PRODUCT_RMS = SalesModel.ItemField.COD_PRODUCT_RMS
  val QTY_ITEM = SalesModel.ItemField.QTY_ITEM
  val VAL_UNIT_PRICE = SalesModel.ItemField.VAL_UNIT_PRICE
  val VAL_TOTAL_WITH_DISCOUNT = SalesModel.ItemField.VAL_TOTAL_WITH_DISCOUNT
  val COD_BUS = SalesModel.ItemField.COD_BUS 
  val DS_SECTOR = SalesModel.ItemField.DS_SECTOR
  val DS_DEPARTMENT = SalesModel.ItemField.DS_DEPARTMENT
  val DS_GROUP = SalesModel.ItemField.DS_GROUP
  val ITEM_NUM_BARCODE = SalesModel.ItemField.NUM_BARCODE
  
  val ItemClientSchema: StructType = {
    StructType(
      StructField(COD_PRODUCT_RMS, DecimalType(38,10))
      :: StructField(QTY_ITEM, DecimalType(38,10))
      :: StructField(VAL_UNIT_PRICE, DecimalType(38,10))
      :: StructField(VAL_TOTAL_WITH_DISCOUNT, DecimalType(38,10))
      :: StructField(COD_BUS, StringType)
      :: StructField(DS_SECTOR, StringType)
      :: StructField(DS_DEPARTMENT, StringType)
      :: StructField(DS_GROUP, StringType)
      :: StructField(ITEM_NUM_BARCODE, DecimalType(38,10))
      :: Nil
    )
  }
  
  val COUPON_NUM_BARCODE = SalesModel.CouponField.NUM_BARCODE
  val QTD_REWARD = SalesModel.CouponField.QTD_REWARD

  val ItemCouponSchema: StructType = {
    StructType(
      StructField(COUPON_NUM_BARCODE, DecimalType(38,10))
      :: StructField(QTD_REWARD, DecimalType(38,10))
      :: Nil
    )
  }
  
  val ID_SOURCE_SALE = SalesModel.Field.ID_SOURCE_SALE
  val DS_SITE_FORMAT = SalesModel.Field.DS_SITE_FORMAT
  val COD_IND_SITE_ECOMM = SalesModel.Field.COD_IND_SITE_ECOMM
  val LST_SALES_ITEMS = SalesModel.Field.LST_SALES_ITEMS
  val COD_DELIVERY = SalesModel.Field.COD_DELIVERY
  val VAL_DOCUMENT_TOTAL_WITH_SERVICES = SalesModel.Field.VAL_DOCUMENT_TOTAL_WITH_SERVICES
  val QTY_TOTAL_COUPON_BURNT = SalesModel.Field.QTY_TOTAL_COUPON_BURNT
  val LST_COUPON_BURNT = SalesModel.Field.LST_COUPON_BURNT
  val IND_IDENTIFIED = SalesModel.Field.IND_IDENTIFIED
  val IND_REGISTERED = SalesModel.Field.IND_REGISTERED
  val NUM_CONSUMER_DOCUMENT_PRIORITY = SalesModel.Field.NUM_CONSUMER_DOCUMENT_PRIORITY
  
  val ClientSchema: StructType = {
    StructType(
      StructField(ID_SOURCE_SALE, IntegerType)
      :: StructField(DS_SITE_FORMAT, StringType)
      :: StructField(COD_IND_SITE_ECOMM, IntegerType)
      :: StructField(LST_SALES_ITEMS, ArrayType(ItemClientSchema))
      :: StructField(COD_DELIVERY, IntegerType)
      :: StructField(VAL_DOCUMENT_TOTAL_WITH_SERVICES, DecimalType(38,10))
      :: StructField(QTY_TOTAL_COUPON_BURNT, DecimalType(38,10))
      :: StructField(IND_IDENTIFIED, IntegerType)
      :: StructField(IND_REGISTERED, IntegerType)
      :: StructField(NUM_CONSUMER_DOCUMENT_PRIORITY, DecimalType(38,10))
      :: Nil
    )
  }

  val ResultSchema = ArrayType(DoubleType)
 
}
