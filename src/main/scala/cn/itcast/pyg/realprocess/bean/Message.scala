package cn.itcast.pyg.realprocess.bean

case class Message (
                     var count:Int = 0,
                     var timeStamp: Long = 0L,
                     var userScan: UserScan = null
                   )
