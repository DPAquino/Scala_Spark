package br.com.spark.business.transform

// built-in
// thrid party
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
// local
import br.com.spark.model.UdafModel
import br.com.spark.utils.ConfigUtil

abstract class Transform {

  val dropUseless = udf((xs: Seq[Row]) => { 
      xs.map{
        row =>
          Row(
            row.getAs(UdafModel.COD_PRODUCT_RMS)
            , row.getAs(UdafModel.QTY_ITEM)
            , row.getAs(UdafModel.VAL_UNIT_PRICE)
            , row.getAs(UdafModel.VAL_TOTAL_WITH_DISCOUNT)
            , row.getAs(UdafModel.COD_BUS)
            , row.getAs(UdafModel.DS_SECTOR)
            , row.getAs(UdafModel.DS_DEPARTMENT)
            , row.getAs(UdafModel.DS_GROUP)
            , row.getAs(UdafModel.ITEM_NUM_BARCODE)
          )
      }
    }, ArrayType(UdafModel.ItemClientSchema)
  )
  
  val getkpiGroup = udf((xs: String) => { 
      val allKpi: Seq[String] = Seq("hipermercados","supermercados","express","drogaria","postos","e-commerce")
      try {
        val index = allKpi.indexOf("supermercados")
        val format = allKpi(index).toUpperCase
        val newAllKpi = allKpi.patch(index, Seq(format), 1)
        val combinations = newAllKpi.toSet.subsets.filter(_.contains(format)).map(m => m.toSeq.sortWith(_.toUpperCase < _.toUpperCase).mkString("|")).toSeq
        combinations
      } catch {
        case e: Exception => {
          Seq(xs)
        }
      }
    }
  )

  def transform(dataframe: DataFrame): DataFrame
}
