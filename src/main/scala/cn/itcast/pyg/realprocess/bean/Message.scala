package cn.itcast.pyg.realprocess.bean

case class Message (
                     var count:Long = 0,
                     var timeStamp: Long = 0L,
                     var userScan: UserScan = null
                   )
