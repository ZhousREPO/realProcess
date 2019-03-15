package cn.itcast.pyg.realprocess.bean

import com.alibaba.fastjson.{JSON, JSONObject}

case class UserScan (
                      var browserType: String = null,
                      var categoryID:String = null,
                      var channelID:String = null,
                      var city: String = null,
                      var country: String = null,
                      var entryTime:String = null,
                      var leaveTime:String = null,
                      var network: String = null,
                      var produceID:String = null,
                      var province: String = null,
                      var source: String = null,
                      var userID:String = null
                    )

object UserScan{
  //将json字符串转换为UserScan
  def toBean(json: String): UserScan = {
    val value: JSONObject = JSON.parseObject(json)
    val browserType: String = value.get("browserType").toString
    val categoryID: String = value.get("categoryID").toString
    val channelID: String = value.get("channelID").toString
    val city: String = value.get("city").toString
    val country: String = value.get("country").toString
    val entryTime: String = value.get("entryTime").toString
    val leaveTime: String = value.get("leaveTime").toString
    val network: String = value.get("network").toString
    val produceID: String = value.get("produceID").toString
    val province: String = value.get("province").toString
    val source: String = value.get("source").toString
    val userID: String = value.get("userID").toString

    UserScan(
      browserType,
      categoryID,
      channelID,
      city,
      country,
      entryTime,
      leaveTime,
      network,
      produceID,
      province,
      source,
      userID

    )

  }
}
