package cn.itcast.pyg.realprocess.utils

import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat

object TimeUtils {
  val SECONDS_PATTERN = "yyyyMMddhhmmss"
  val HOUR_PATTERN = "yyyyMMddhh"
  val DAY_PATTERN = "yyyyMMdd"
  val MONTH_PATTERN = "yyyyMM"

  //将时间戳转换成固定格式时间字符串
  def timeStampToStr(timeStamp:Long,pattern:String): String = {
    val fastDateFormat: FastDateFormat = FastDateFormat.getInstance(pattern)
    val timeStr: String = fastDateFormat.format(timeStamp)
    timeStr
  }

  //将时间戳转换成对应格式向下取整的时间戳,先转成str再转成时间戳
  def timeStampToFormatTimeStamp(timeStamp:Long,pattern:String): Long = {
    val fastDateFormat: FastDateFormat = FastDateFormat.getInstance(pattern)
    val date: Date = fastDateFormat.parse(timeStampToStr(timeStamp,pattern))
    date.getTime
  }
}
