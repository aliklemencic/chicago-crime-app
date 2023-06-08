import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.util.Bytes

import java.lang.System.console

object StreamWeather {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  val hbaseConf: Configuration = HBaseConfiguration.create()
  hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
  hbaseConf.set("hbase.zookeeper.quorum", "localhost")

  val hbaseConnection: Connection = ConnectionFactory.createConnection(hbaseConf)
  val table: Table = hbaseConnection.getTable(TableName.valueOf("aliklemencic_latest_weather"))
  
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(s"""
        |Usage: aliklemencicStreamWeather <brokers>
        |  <brokers> is a list of one or more Kafka brokers
        | 
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers) = args
    val sparkConf = new SparkConf().setAppName("aliklemencicStreamWeather")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val topicsSet = Set("aliklemencic-weather-reports")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent,
      Subscribe[String, String](topicsSet, kafkaParams)
    )

    val serializedRecords = stream.map(_.value);
    val reports = serializedRecords.map(rec => mapper.readValue(rec, classOf[aliklemencicWeatherReport]))

    val batchStats = reports.map(wr => {
      val date = wr.day + "/" + wr.month + "/" + wr.year
      val put = new Put(Bytes.toBytes(date))
      put.addColumn(Bytes.toBytes("weather"), Bytes.toBytes("month"), Bytes.toBytes(wr.month))
      put.addColumn(Bytes.toBytes("weather"), Bytes.toBytes("year"), Bytes.toBytes(wr.year))
      put.addColumn(Bytes.toBytes("weather"), Bytes.toBytes("clear"), Bytes.toBytes(wr.clear))
      put.addColumn(Bytes.toBytes("weather"), Bytes.toBytes("fog"), Bytes.toBytes(wr.fog))
      put.addColumn(Bytes.toBytes("weather"), Bytes.toBytes("rain"), Bytes.toBytes(wr.rain))
      put.addColumn(Bytes.toBytes("weather"), Bytes.toBytes("snow"), Bytes.toBytes(wr.snow))
      put.addColumn(Bytes.toBytes("weather"), Bytes.toBytes("hail"), Bytes.toBytes(wr.hail))
      put.addColumn(Bytes.toBytes("weather"), Bytes.toBytes("thunder"), Bytes.toBytes(wr.thunder))
      put.addColumn(Bytes.toBytes("weather"), Bytes.toBytes("tornado"), Bytes.toBytes(wr.tornado))
      table.put(put)
    })
    batchStats.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
