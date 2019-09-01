package br.com.spark

import org.apache.spark.sql.{SparkSession, Row, DataFrame}
import org.apache.spark.sql.types._
import br.com.spark.model._

object Mock {

  val obj_Struct: StructType =
    StructType(
      StructField(UdafModel.NUM_CONSUMER_DOCUMENT_PRIORITY, DecimalType(38, 10))
      :: StructField(SalesModel.Field.COD_SITE, StringType)
      :: StructField(MetricsModel.Field.COD_UF, IntegerType)
      :: StructField(UdafModel.ID_SOURCE_SALE, IntegerType)
      :: StructField(UdafModel.DS_SITE_FORMAT, StringType)
      :: StructField(UdafModel.COD_IND_SITE_ECOMM, IntegerType)
      :: StructField(UdafModel.LST_SALES_ITEMS, ArrayType(UdafModel.ItemClientSchema))
      :: StructField(UdafModel.COD_DELIVERY, IntegerType)
      :: StructField(UdafModel.VAL_DOCUMENT_TOTAL_WITH_SERVICES, DecimalType(38, 10))
      :: StructField(UdafModel.QTY_TOTAL_COUPON_BURNT, DecimalType(38, 10))
      :: StructField(UdafModel.LST_COUPON_BURNT, ArrayType(UdafModel.ItemCouponSchema))
      :: StructField(UdafModel.IND_IDENTIFIED, IntegerType)
      :: StructField(UdafModel.IND_REGISTERED, IntegerType)
      :: Nil
    )

  val obj_Data: Seq[Row] = Seq(
    Row(
      scala.math.BigDecimal(1234567890.0), "123", 11, 1, "HIPERMERCADOS", 0
      , Seq(
        Row(scala.math.BigDecimal(123) , scala.math.BigDecimal(1.00) , scala.math.BigDecimal(1.50) , scala.math.BigDecimal(2.00) , "FOOD" , "" , "" , "", scala.math.BigDecimal(12345))
        , Row(scala.math.BigDecimal(123) , scala.math.BigDecimal(1.00) , scala.math.BigDecimal(1.50) , scala.math.BigDecimal(2.00) , "FOOD" , "" , "" , "", scala.math.BigDecimal(12345))
      )
      , 1, scala.math.BigDecimal(10.0), scala.math.BigDecimal(5.0)
      , Seq(
        Row(scala.math.BigDecimal(12345), scala.math.BigDecimal(1))
        , Row(scala.math.BigDecimal(12345), scala.math.BigDecimal(2))
      )
      , 0, 0
    )
    , Row(
      scala.math.BigDecimal(1234567890.0), "123", 11, 1, "HIPERMERCADOS|supermercados", 0
      , Seq(
        Row(scala.math.BigDecimal(123) , scala.math.BigDecimal(1.00) , scala.math.BigDecimal(1.50) , scala.math.BigDecimal(2.00) , "FOOD" , "" , "" , "", scala.math.BigDecimal(12345))
        , Row(scala.math.BigDecimal(123) , scala.math.BigDecimal(1.00) , scala.math.BigDecimal(1.50) , scala.math.BigDecimal(2.00) , "FOOD" , "" , "" , "", scala.math.BigDecimal(12345))
      )
      , 1, scala.math.BigDecimal(10.0), scala.math.BigDecimal(5.0)
      , Seq(
        Row(scala.math.BigDecimal(12345), scala.math.BigDecimal(1))
        , Row(scala.math.BigDecimal(12345), scala.math.BigDecimal(2))
      )
      , 0, 0
    )
    , Row(
      scala.math.BigDecimal(1234567890.0), "123", 11, 1, "hipermercados|SUPERMERCADOS", 0
      , Seq(
        Row(scala.math.BigDecimal(123) , scala.math.BigDecimal(1.00) , scala.math.BigDecimal(1.50) , scala.math.BigDecimal(0.75) , "FOOD" , "" , "" , "", scala.math.BigDecimal(12345))
        , Row(scala.math.BigDecimal(123) , scala.math.BigDecimal(1.00) , scala.math.BigDecimal(1.50) , scala.math.BigDecimal(0.75) , "FOOD" , "" , "" , "", scala.math.BigDecimal(12345))
      )
      , 1, scala.math.BigDecimal(10.0), scala.math.BigDecimal(5.0)
      , Seq(
        Row(scala.math.BigDecimal(12345), scala.math.BigDecimal(1))
        , Row(scala.math.BigDecimal(12345), scala.math.BigDecimal(2))
      )
      , 0, 0
    )
  )

  val results: Map[String, Seq[String]] = Map(
    "1|1"     -> Seq("3.17", "3.17", "4.00", "1.50", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "3.17", "0.00")
    , "1|2"   -> Seq("1.58", "1.58", "2.00", "0.75", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "1.58", "0.00")
    , "1|3"   -> Seq("3.00", "3.00", "2.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "3.00", "0.00")
    , "1|4"   -> Seq("9.50", "9.50", "8.00", "1.50", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "9.50", "0.00")
    , "1|5"   -> Seq("3.00", "3.00", "2.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "3.00", "0.00")
    , "1|6"   -> Seq("6.00", "6.00", "4.00", "2.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "6.00", "0.00")
    , "1|7"   -> Seq("1.00", "1.00", "1.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "1.00", "0.00")
    , "1|8"   -> Seq("2.00", "2.00", "2.00", "2.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "2.00", "0.00")
    , "1|9"   -> Seq("1.00", "1.00", "1.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "1.00", "0.00")
    , "1|10"  -> Seq("1.00", "1.00", "1.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "1.00", "0.00")
    , "1|11"  -> Seq("1.00", "1.00", "1.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "1.00", "0.00")
    , "1|12"  -> Seq("2.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "3.00", "0.00")
    , "1|13"  -> Seq("0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00")
    , "1|14"  -> Seq("2.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "3.00", "0.00")
    , "1|15"  -> Seq("15.00", "15.00", "10.00", "5.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "15.00", "0.00")
    , "1|16"  -> Seq("15.00", "15.00", "10.00", "5.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "15.00", "0.00")
    , "2|1"     -> Seq("3.17", "3.17", "4.00", "1.50", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "3.17", "0.00")
    , "2|2"   -> Seq("1.58", "1.58", "2.00", "0.75", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "1.58", "0.00")
    , "2|3"   -> Seq("3.00", "3.00", "2.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "3.00", "0.00")
    , "2|4"   -> Seq("9.50", "9.50", "8.00", "1.50", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "9.50", "0.00")
    , "2|5"   -> Seq("3.00", "3.00", "2.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "3.00", "0.00")
    , "2|6"   -> Seq("6.00", "6.00", "4.00", "2.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "6.00", "0.00")
    , "2|7"   -> Seq("1.00", "1.00", "1.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "1.00", "0.00")
    , "2|8"   -> Seq("2.00", "2.00", "2.00", "2.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "2.00", "0.00")
    , "2|9"   -> Seq("1.00", "1.00", "1.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "1.00", "0.00")
    , "2|10"  -> Seq("1.00", "1.00", "1.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "1.00", "0.00")
    , "2|11"  -> Seq("1.00", "1.00", "1.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "1.00", "0.00")
    , "2|12"  -> Seq("2.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "3.00", "0.00")
    , "2|13"  -> Seq("0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00")
    , "2|14"  -> Seq("2.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "3.00", "0.00")
    , "2|15"  -> Seq("15.00", "15.00", "10.00", "5.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "15.00", "0.00")
    , "2|16"  -> Seq("15.00", "15.00", "10.00", "5.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "15.00", "0.00")
    , "3|1"     -> Seq("3.17", "3.17", "4.00", "1.50", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "3.17", "0.00")
    , "3|2"   -> Seq("1.58", "1.58", "2.00", "0.75", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "1.58", "0.00")
    , "3|3"   -> Seq("3.00", "3.00", "2.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "3.00", "0.00")
    , "3|4"   -> Seq("9.50", "9.50", "8.00", "1.50", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "9.50", "0.00")
    , "3|5"   -> Seq("3.00", "3.00", "2.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "3.00", "0.00")
    , "3|6"   -> Seq("6.00", "6.00", "4.00", "2.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "6.00", "0.00")
    , "3|7"   -> Seq("1.00", "1.00", "1.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "1.00", "0.00")
    , "3|8"   -> Seq("2.00", "2.00", "2.00", "2.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "2.00", "0.00")
    , "3|9"   -> Seq("1.00", "1.00", "1.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "1.00", "0.00")
    , "3|10"  -> Seq("1.00", "1.00", "1.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "1.00", "0.00")
    , "3|11"  -> Seq("1.00", "1.00", "1.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "1.00", "0.00")
    , "3|12"  -> Seq("2.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "3.00", "0.00")
    , "3|13"  -> Seq("0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00")
    , "3|14"  -> Seq("2.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "3.00", "0.00")
    , "3|15"  -> Seq("15.00", "15.00", "10.00", "5.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "15.00", "0.00")
    , "3|16"  -> Seq("15.00", "15.00", "10.00", "5.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "15.00", "0.00")
    , "4|1"     -> Seq("3.17", "3.17", "4.00", "1.50", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "3.17", "0.00")
    , "4|2"   -> Seq("1.58", "1.58", "2.00", "0.75", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "1.58", "0.00")
    , "4|3"   -> Seq("3.00", "3.00", "2.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "3.00", "0.00")
    , "4|4"   -> Seq("9.50", "9.50", "8.00", "1.50", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "9.50", "0.00")
    , "4|5"   -> Seq("3.00", "3.00", "2.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "3.00", "0.00")
    , "4|6"   -> Seq("6.00", "6.00", "4.00", "2.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "6.00", "0.00")
    , "4|7"   -> Seq("1.00", "1.00", "1.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "1.00", "0.00")
    , "4|8"   -> Seq("2.00", "2.00", "2.00", "2.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "2.00", "0.00")
    , "4|9"   -> Seq("1.00", "1.00", "1.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "1.00", "0.00")
    , "4|10"  -> Seq("1.00", "1.00", "1.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "1.00", "0.00")
    , "4|11"  -> Seq("1.00", "1.00", "1.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "1.00", "0.00")
    , "4|12"  -> Seq("2.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "3.00", "0.00")
    , "4|13"  -> Seq("0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00")
    , "4|14"  -> Seq("2.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "3.00", "0.00")
    , "4|15"  -> Seq("15.00", "15.00", "10.00", "5.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "15.00", "0.00")
    , "4|16"  -> Seq("15.00", "15.00", "10.00", "5.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "15.00", "0.00")
    , "5|1"   -> Seq("3.17", "3.17", "4.00", "1.50", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "3.17", "0.00")
    , "5|2"   -> Seq("1.58", "1.58", "2.00", "0.75", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "1.58", "0.00")
    , "5|3"   -> Seq("3.00", "3.00", "2.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "3.00", "0.00")
    , "5|4"   -> Seq("9.50", "9.50", "8.00", "1.50", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "9.50", "0.00")
    , "5|5"   -> Seq("3.00", "3.00", "2.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "3.00", "0.00")
    , "5|6"   -> Seq("6.00", "6.00", "4.00", "2.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "6.00", "0.00")
    , "5|7"   -> Seq("1.00", "1.00", "1.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "1.00", "0.00")
    , "5|8"   -> Seq("2.00", "2.00", "2.00", "2.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "2.00", "0.00")
    , "5|9"   -> Seq("1.00", "1.00", "1.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "1.00", "0.00")
    , "5|10"  -> Seq("1.00", "1.00", "1.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "1.00", "0.00")
    , "5|11"  -> Seq("1.00", "1.00", "1.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "1.00", "0.00")
    , "5|12"  -> Seq("2.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "3.00", "0.00")
    , "5|13"  -> Seq("0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00")
    , "5|14"  -> Seq("2.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "3.00", "0.00")
    , "5|15"  -> Seq("15.00", "15.00", "10.00", "5.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "15.00", "0.00")
    , "5|16"  -> Seq("15.00", "15.00", "10.00", "5.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "15.00", "0.00")
    , "6|1"   -> Seq("3.17", "3.17", "4.00", "1.50", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "3.17", "0.00")
    , "6|2"   -> Seq("1.58", "1.58", "2.00", "0.75", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "1.58", "0.00")
    , "6|3"   -> Seq("3.00", "3.00", "2.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "3.00", "0.00")
    , "6|4"   -> Seq("9.50", "9.50", "8.00", "1.50", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "9.50", "0.00")
    , "6|5"   -> Seq("3.00", "3.00", "2.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "3.00", "0.00")
    , "6|6"   -> Seq("6.00", "6.00", "4.00", "2.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "6.00", "0.00")
    , "6|7"   -> Seq("1.00", "1.00", "1.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "1.00", "0.00")
    , "6|8"   -> Seq("2.00", "2.00", "2.00", "2.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "2.00", "0.00")
    , "6|9"   -> Seq("1.00", "1.00", "1.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "1.00", "0.00")
    , "6|10"  -> Seq("1.00", "1.00", "1.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "1.00", "0.00")
    , "6|11"  -> Seq("1.00", "1.00", "1.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "1.00", "0.00")
    , "6|12"  -> Seq("2.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "3.00", "0.00")
    , "6|13"  -> Seq("0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00")
    , "6|14"  -> Seq("2.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "3.00", "0.00")
    , "6|15"  -> Seq("15.00", "15.00", "10.00", "5.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "15.00", "0.00")
    , "6|16"  -> Seq("15.00", "15.00", "10.00", "5.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "15.00", "0.00")
    , "7|1"   -> Seq("3.17", "3.17", "4.00", "1.50", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "3.17", "0.00")
    , "7|2"   -> Seq("1.58", "1.58", "2.00", "0.75", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "1.58", "0.00")
    , "7|3"   -> Seq("3.00", "3.00", "2.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "3.00", "0.00")
    , "7|4"   -> Seq("9.50", "9.50", "8.00", "1.50", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "9.50", "0.00")
    , "7|5"   -> Seq("3.00", "3.00", "2.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "3.00", "0.00")
    , "7|6"   -> Seq("6.00", "6.00", "4.00", "2.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "6.00", "0.00")
    , "7|7"   -> Seq("1.00", "1.00", "1.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "1.00", "0.00")
    , "7|8"   -> Seq("2.00", "2.00", "2.00", "2.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "2.00", "0.00")
    , "7|9"   -> Seq("1.00", "1.00", "1.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "1.00", "0.00")
    , "7|10"  -> Seq("1.00", "1.00", "1.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "1.00", "0.00")
    , "7|11"  -> Seq("1.00", "1.00", "1.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "1.00", "0.00")
    , "7|12"  -> Seq("2.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "3.00", "0.00")
    , "7|13"  -> Seq("0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00")
    , "7|14"  -> Seq("2.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "3.00", "0.00")
    , "7|15"  -> Seq("15.00", "15.00", "10.00", "5.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "15.00", "0.00")
    , "7|16"  -> Seq("15.00", "15.00", "10.00", "5.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "15.00", "0.00")
    , "8|1"   -> Seq("3.17", "3.17", "4.00", "1.50", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "3.17", "0.00")
    , "8|2"   -> Seq("1.58", "1.58", "2.00", "0.75", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "1.58", "0.00")
    , "8|3"   -> Seq("3.00", "3.00", "2.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "3.00", "0.00")
    , "8|4"   -> Seq("9.50", "9.50", "8.00", "1.50", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "9.50", "0.00")
    , "8|5"   -> Seq("3.00", "3.00", "2.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "3.00", "0.00")
    , "8|6"   -> Seq("6.00", "6.00", "4.00", "2.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "6.00", "0.00")
    , "8|7"   -> Seq("1.00", "1.00", "1.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "1.00", "0.00")
    , "8|8"   -> Seq("2.00", "2.00", "2.00", "2.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "2.00", "0.00")
    , "8|9"   -> Seq("1.00", "1.00", "1.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "1.00", "0.00")
    , "8|10"  -> Seq("1.00", "1.00", "1.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "1.00", "0.00")
    , "8|11"  -> Seq("1.00", "1.00", "1.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "1.00", "0.00")
    , "8|12"  -> Seq("2.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "3.00", "0.00")
    , "8|13"  -> Seq("0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00")
    , "8|14"  -> Seq("2.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "3.00", "0.00")
    , "8|15"  -> Seq("15.00", "15.00", "10.00", "5.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "15.00", "0.00")
    , "8|16"  -> Seq("15.00", "15.00", "10.00", "5.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "15.00", "0.00")
    , "11|1"   -> Seq("3.17", "3.17", "4.00", "1.50", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "3.17", "0.00")
    , "11|2"   -> Seq("1.58", "1.58", "2.00", "0.75", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "1.58", "0.00")
    , "11|3"   -> Seq("3.00", "3.00", "2.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "3.00", "0.00")
    , "11|4"   -> Seq("9.50", "9.50", "8.00", "1.50", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "9.50", "0.00")
    , "11|5"   -> Seq("3.00", "3.00", "2.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "3.00", "0.00")
    , "11|6"   -> Seq("6.00", "6.00", "4.00", "2.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "6.00", "0.00")
    , "11|7"   -> Seq("1.00", "1.00", "1.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "1.00", "0.00")
    , "11|8"   -> Seq("2.00", "2.00", "2.00", "2.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "2.00", "0.00")
    , "11|9"   -> Seq("1.00", "1.00", "1.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "1.00", "0.00")
    , "11|10"  -> Seq("1.00", "1.00", "1.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "1.00", "0.00")
    , "11|11"  -> Seq("1.00", "1.00", "1.00", "1.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "1.00", "0.00")
    , "11|12"  -> Seq("2.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "3.00", "0.00")
    , "11|13"  -> Seq("0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00")
    , "11|14"  -> Seq("2.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "3.00", "0.00")
    , "11|15"  -> Seq("15.00", "15.00", "10.00", "5.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "15.00", "0.00")
    , "11|16"  -> Seq("15.00", "15.00", "10.00", "5.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "15.00", "0.00")
  )
    
  def apply(spark:SparkSession):DataFrame = {
    spark.sqlContext.createDataFrame(spark.sparkContext.parallelize(obj_Data), obj_Struct)
  }

}

