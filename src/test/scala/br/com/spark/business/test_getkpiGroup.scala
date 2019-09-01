package br.com.spark.business

// standard library imports
// related third party imports
/* ScalaCheck ScalaTest */
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers
// local application/library specific imports

class test_getkpiGroup extends FunSuite with Checkers {
  test("[getkpiGroup] Combinantions supermercados") {
    val allKpi: Seq[String] = Seq("hipermercados","supermercados","express","drogaria","postos","e-commerce")
    val index = allKpi.indexOf("supermercados")
    val format = allKpi(index).toUpperCase
    val newAllKpi = allKpi.patch(index, Seq(format), 1)
    val combinationsList = newAllKpi.toSet.subsets.filter(_.contains(format)).map(m => m.toSeq.sortWith(_.toUpperCase < _.toUpperCase).mkString("|")).toSeq
    combinationsList.foreach(println)
    println(combinationsList.size)
  }
  test("[getkpiGroup] Combinantions hipermercados") {
    val allKpi: Seq[String] = Seq("hipermercados","supermercados","express","drogaria","postos","e-commerce")
    val index = allKpi.indexOf("hipermercados")
    val format = allKpi(index).toUpperCase
    val newAllKpi = allKpi.patch(index, Seq(format), 1)
    val combinationsList = newAllKpi.toSet.subsets.filter(_.contains(format)).map(m => m.toSeq.sortWith(_.toUpperCase < _.toUpperCase).mkString("|")).toSeq
    combinationsList.foreach(println)
    println(combinationsList.size)
  }
}

