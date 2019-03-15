package cn.itcast.pyg.realprocess

import cn.itcast.pyg.realprocess.bean.Message
import cn.itcast.pyg.realprocess.utils.KafkaConfig
import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSource}
import org.apache.flink.streaming.api.environment.{CheckpointConfig, LocalStreamEnvironment, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09

object App {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //设置checkpoint
    //开启Checkpoint
    env.enableCheckpointing(5000) //每隔5秒,checkpoint1次
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(1000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    //当程序关闭的时候,会不会触发额外的checkpoint
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.setStateBackend(new FsStateBackend("hdfs://node01:8020/flink-checkpoint"))

    //创建kafka数据源
    val kafkaSource: FlinkKafkaConsumer09[String] = new FlinkKafkaConsumer09[String](KafkaConfig.topic,
      new SimpleStringSchema(),
      KafkaConfig.getProperties)

    //添加kafka数据源到flink env
    //从kafka获取数据
    val source: DataStream[String] = env.addSource(kafkaSource)

    //解析数据源
    val parsedSource = source.map {
      strip =>
        val jsonObj = JSON.parseObject(strip)

        Message()
    }

    //设置flink水印的时间划分方式
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //设置水印







    //启动flink流
    env.execute()
  }
}
