package cn.itcast.pyg.realprocess.bean

case class ChannelPVUV (
    channelID: String,
    userID: String,
    timeStamp: Long,
    pv: Long,
    uv: Long,
    //数据字段
    dataField: String,
    //聚合字段
    aggregateField: String
)
