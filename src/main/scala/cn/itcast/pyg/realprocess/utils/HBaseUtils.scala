package cn.itcast.pyg.realprocess.utils

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

/*
hbase.zookeeper.quorum="node01:2181,node02:2181,node03:2181"
hbase.master="node01:60000"
hbase.zookeeper.property.clientPort="2181"
hbase.rpc.timeout="600000"
hbase.client.operator.timeout="600000"
hbase.client.scanner.timeout.period="600000"
 */
object HBaseUtils {
  //从conf文件加载配置数据
  private val conf: Config = ConfigFactory.load()
  //加载HBase配置
  private val configuration: Configuration = HBaseConfiguration.create()

  configuration.set("hbase.zookeeper.quorum",conf.getString("hbase.zookeeper.quorum"))
  configuration.set("hbase.master", conf.getString("hbase.master"))
  configuration.set("hbase.zookeeper.property.clientPort", conf.getString("hbase.zookeeper.property.clientPort"))
  configuration.set("hbase.rpc.timeout", conf.getString("hbase.rpc.timeout"))
  configuration.set("hbase.client.operator.timeout", conf.getString("hbase.client.operator.timeout"))
  configuration.set("hbase.client.scanner.timeout.period", conf.getString("hbase.client.scanner.timeout.period"))

  //创建连接
  private val conn: Connection = ConnectionFactory.createConnection(configuration)
  //列族名称
  private val columnFamily = "info"

  //获取表，之后操作数据
  def getTable(tableNameStr:String): Table = {
    val tableName: TableName = TableName.valueOf(tableNameStr)
    //通过conn获取到admin
    val admin = conn.getAdmin
    //判断表是否存在
    if(!admin.tableExists(tableName)) {
      val hTableDescriptor = new HTableDescriptor(tableName)
      val HColumnDescriptor = new HColumnDescriptor(columnFamily)
      hTableDescriptor.addFamily(HColumnDescriptor)
      //不存在,创建表
      admin.createTable(hTableDescriptor)
    }
    val table: Table = conn.getTable(tableName)
    table
  }

  //查询表中数据
  def getFromHBase(tableName:String,rowKey:String,column:String): String = {
    val table: Table = getTable(tableName)
    val get = new Get(rowKey.getBytes())
    val resultSet: Result = table.get(get)

    val queryResult: Array[Byte] = resultSet.getValue(columnFamily.getBytes(),column.getBytes())
    val queryOption: Option[Array[Byte]] = Option(queryResult)
    val result: String = queryOption match {
        //注意使用了Bytes.toString的方法将Array[Byte]类型转成String
      case Some(a) => Bytes.toString(a)
      case None => ""
    }
    result
  }

  //向表中添加数据
  def putToHBase(tableName:String,rowKey:String,column:String,data:String): Unit = {
    val table: Table = getTable(tableName)

    val put = new Put(rowKey.getBytes())
    put.addColumn(columnFamily.getBytes(),column.getBytes(),data.getBytes())
    table.put(put)
  }

  //批量向表中添加数据
  def putBatchToHBase(tableName:String,rowKey:String,dataSet:Map[String,String]): Unit ={
    val table = getTable(tableName)

    val put = new Put(rowKey.getBytes())
    for ((key,value) <- dataSet) {
      put.add(columnFamily.getBytes(),key.getBytes(),value.getBytes())
    }
    table.put(put)
  }

  def main(args: Array[String]): Unit = {
    val map = Map("name"->"zhangsan",
      "age"->"23")
    //    putBatchToHBase("test001","rk0001",map)
        val result: String = getFromHBase("test001","rk0001","name")
      println(result)
  }
}
