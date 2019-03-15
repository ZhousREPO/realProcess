package cn.itcast.pyg.realprocess.utils

import org.apache.commons.lang3.StringUtils

/*
给用户做标记，第一次到将firstTime和lastTime存放到表中
不是第一次到取出lastTime计算出是在一个小时之内、一天之内还是一月之内到达
 */
case class UserState (
                       var isNew: Boolean = false,
                       var isHour: Boolean = false,
                       var isDay: Boolean = false,
                       var isMonth: Boolean = false
                     )
object UserStateUtils {

  def getUserState(userID: String, timeStamp: Long): UserState = {
    //将user的第一次访问时间,和最后一次访问时间,存入HBase,
    //查询这个时间,进行对比,这样就可以知道,userstate的状态了
    val tableName = "userstate" // 用来存储,用户的访问时间记录
    val rowKey = userID
    val firstColumn = "firstColumn"
    val lastColumn = "lastColumn"

    //默认都为false
    var isNew = false
    var isHour = false
    var isDay = false
    var isMonth = false

    //获取最后一次访问时间
    val lastData: String = HBaseUtils.getFromHBase(tableName, rowKey, lastColumn)

    if (StringUtils.isNotBlank(lastData)) {
      //不是新来的,老鸟
      //是不是这个小时的第一次呀?
      //如果本次过来的时间,向下取整,还比数据库的时间大,那么就认为是本阶段第一次过来
      //传过来的时间戳:      201903150909   -> 201903150900
      //数据库里面查到的数据: 201903150859   ->  和上面取整之后的数据比大小,如果数据库的数据小?肯定是上一个小时的数据
      if (TimeUtils.timeStampToFormatTimeStamp(timeStamp, TimeUtils.HOUR_PATTERN) > lastData.toLong) {
        isHour = true
      }
      //是不是这一天的第一次?
      if (TimeUtils.timeStampToFormatTimeStamp(timeStamp, TimeUtils.DAY_PATTERN) > lastData.toLong) {
        isDay = true
      }
      //是不是这一月的第一次?
      if (TimeUtils.timeStampToFormatTimeStamp(timeStamp, TimeUtils.MONTH_PATTERN) > lastData.toLong) {
        isMonth = true
      }
      //老用户访问,记录本次访问时间
      HBaseUtils.putToHBase(tableName, rowKey, lastColumn, timeStamp.toString)
    } else {
      //是新用户
      isNew = true
      isHour = true
      isDay = true
      isMonth = true
      var map = Map[String, String]()
      map += (firstColumn -> timeStamp)
      map += (lastColumn -> timeStamp)
      //新用户访问,记录访问时间.
      HBaseUtils.putBatchToHBase(tableName, rowKey, map)
    }

    //将最终结果封装到UserState
    UserState(isNew, isHour, isDay, isMonth)
  }
}
