package cn.itcast.pyg.realprocess.utils

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}

import scala.util.Properties

/*
bootstrap.servers="node01:9092,node02:9092,node03:9092"
zookeeper.connect="node01:2181,node02:2181,node03:2181"
input.topic="pyg"
gruop.id="pyg"
enable.auto.commit="true"
auto.commit.interval.ms="5000"
auto.offset.reset="latest"
key.serializer="org.apache.kafka.common.serialization.StringSerializer"
key.deserializer="org.apache.kafka.common.serialization.StringDeserializer"
 */
object KafkaConfig {
  val conf: Config = ConfigFactory.load()
  val topic: String = conf.getString("input.topic")

  def getProperties: Properties = {
    var properties = new Properties()

    properties.setProperty("bootstrap.servers", conf.getString("bootstrap.servers"))
    properties.setProperty("zookeeper.connect", conf.getString("zookeeper.connect"))
    properties.setProperty("input.topic", topic)
    properties.setProperty("gruop.id", conf.getString("gruop.id"))
    properties.setProperty("enable.auto.commit", conf.getString("enable.auto.commit"))
    properties.setProperty("auto.commit.interval.ms", conf.getString("auto.commit.interval.ms"))
    properties.setProperty("auto.offset.reset", conf.getString("auto.offset.reset"))
    properties.setProperty("key.serializer", conf.getString("key.serializer"))
    properties.setProperty("key.deserializer", conf.getString("key.deserializer"))
    properties
  }
}
