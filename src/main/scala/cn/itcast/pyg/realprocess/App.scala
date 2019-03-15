package cn.itcast.pyg.realprocess

import cn.itcast.pyg.realprocess.bean.{Message, UserScan}
import cn.itcast.pyg.realprocess.utils.KafkaConfig
import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09

object App {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //设置flink水印的时间划分方式
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //设置checkpoint
    //开启Checkpoint
    env.enableCheckpointing(5000) //每隔5秒,checkpoint1次
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(1000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    //当程序关闭的时候,会不会触发额外的checkpoint
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env setStateBackend new FsStateBackend("hdfs://node01:8020/flink-checkpoint")

    //创建kafka数据源
    val kafkaSource: FlinkKafkaConsumer09[String] = new FlinkKafkaConsumer09[String](
      KafkaConfig.topic,
      new SimpleStringSchema(),
      KafkaConfig.getProperties
    )

    //添加kafka数据源到flink env
    //从kafka获取数据
    val source: DataStream[String] = env.addSource(kafkaSource)

    //val kafkaDataStream: DataStream[String] = source.forward()

    //解析数据源
    val messageData = source.map {
      line =>
        val jsonObj = JSON.parseObject(line)
        val count = jsonObj.getLong("count")
        val timeStamp = jsonObj.getLong("timeStamp")
        val jsonData = jsonObj.getString("data")
        val userScan = UserScan.toBean(jsonData)
        Message(count, timeStamp, userScan)
    }

    //设置水印
    val watermark = messageData.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[Message] {

      var currentTimestamp = 0L
      val maxDelayTime = 2000L  //设置最大延时2s

      //获取当前水印，此方法是flink调用
      override def getCurrentWatermark: Watermark = {
        new Watermark(currentTimestamp - maxDelayTime)
      }

      //抽取时间戳
      override def extractTimestamp(message: Message, l: Long): Long = {
        val time = message.timeStamp
        //通过max获取到
        currentTimestamp= Math.max(time, currentTimestamp)
        currentTimestamp
      }
    })
    watermark.print()

    //启动flink流
    env.execute("app")
  }
}
