package br.com.spark.utils

import java.text.SimpleDateFormat
import java.util.Calendar
import org.slf4j.LoggerFactory

object DateUtil {
  val log = LoggerFactory.getLogger(this.getClass)

  def getDate(param: String): String = {
    log.info("[*] Getting date")
    val paramDate = Option(param).getOrElse(getCurrentTime())
    log.info("[*] Parameter date is: " + paramDate)
    paramDate
  }

  def getCurrentTime(): String = new SimpleDateFormat("yyyyMMdd").format(Calendar.getInstance().getTime())

  def getYear(date:String) = {
    date.substring(0, 4)
  }

  def getMonth(date:String) = {
    date.substring(4, 6)
  }

  def getFirstDay(date:String) = {
    s"${date.substring(0, 6)}01"
  }

  def getLastDay(date:String) = {
    s"${date.substring(0, 6)}31"
  }

}
