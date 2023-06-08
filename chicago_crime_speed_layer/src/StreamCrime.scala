import StreamWeather.hbaseConnection
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Get, Increment, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._

object StreamCrime {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  val hbaseConf: Configuration = HBaseConfiguration.create()
  hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
  hbaseConf.set("hbase.zookeeper.quorum", "localhost")

  val hbaseConnection: Connection = ConnectionFactory.createConnection(hbaseConf)
  val crimeByWeather: Table = hbaseConnection.getTable(TableName.valueOf("aliklemencic_yearly_crimes_by_weather"))
  val weatherTable: Table = hbaseConnection.getTable(TableName.valueOf("aliklemencic_latest_weather"))
  val crimeTable: Table = hbaseConnection.getTable(TableName.valueOf("aliklemencic_latest_crime"))

  def getWeather(day: String, month: String, year: String): Option[aliklemencicWeatherReport] = {
    val date = day + "/" + month + "/" + year
    val result = weatherTable.get(new Get(Bytes.toBytes(date)))
    System.out.println(result.isEmpty)
    if(result.isEmpty)
      None
    else {
      Some(aliklemencicWeatherReport(
        Bytes.toString(result.getValue(Bytes.toBytes("weather"), Bytes.toBytes("day"))),
        Bytes.toString(result.getValue(Bytes.toBytes("weather"), Bytes.toBytes("month"))),
        Bytes.toString(result.getValue(Bytes.toBytes("weather"), Bytes.toBytes("year"))),
        Bytes.toBoolean(result.getValue(Bytes.toBytes("weather"), Bytes.toBytes("clear"))),
        Bytes.toBoolean(result.getValue(Bytes.toBytes("weather"), Bytes.toBytes("fog"))),
        Bytes.toBoolean(result.getValue(Bytes.toBytes("weather"), Bytes.toBytes("rain"))),
        Bytes.toBoolean(result.getValue(Bytes.toBytes("weather"), Bytes.toBytes("snow"))),
        Bytes.toBoolean(result.getValue(Bytes.toBytes("weather"), Bytes.toBytes("hail"))),
        Bytes.toBoolean(result.getValue(Bytes.toBytes("weather"), Bytes.toBytes("thunder"))),
        Bytes.toBoolean(result.getValue(Bytes.toBytes("weather"), Bytes.toBytes("tornado")))))
    }
  }

  def incrementCrimesByWeather(crime: aliklemencicCrimeReport) : String = {
    val maybeWeather = getWeather(crime.day, crime.month, crime.year)
    if(maybeWeather.isEmpty)
      return "No weather for " + crime.day + "/" + crime.month + "/" + crime.year
    val latestWeather = maybeWeather.get
    val inc = new Increment(Bytes.toBytes(crime.crime_type + crime.year))
    if(latestWeather.clear) {
      inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("clear_crimes"), 1)
    }
    if(latestWeather.fog) {
      inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("fog_crimes"), 1)
    }
    if(latestWeather.rain) {
      inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("rain_crimes"), 1)
    }
    if(latestWeather.snow) {
      inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("snow_crimes"), 1)
    }
    if(latestWeather.hail) {
      inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("hail_crimes"), 1)
    }
    if(latestWeather.thunder) {
      inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("thunder_crimes"), 1)
    }
    if(latestWeather.tornado) {
      inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("tornado_crimes"), 1)
    }
    crimeByWeather.increment(inc)
    return "Updated speed layer for " + crime.crime_type + "crimes from " + crime.day + "/" + crime.month + "/" + crime.year
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(s"""
        |Usage: aliklemencicStreamCrime <brokers>
        |  <brokers> is a list of one or more Kafka brokers
        | 
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers) = args
    val sparkConf = new SparkConf().setAppName("aliklemencicStreamCrime")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val topicsSet = Set("aliklemencic-crime-reports")
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
    val crimeReports = serializedRecords.map(rec => mapper.readValue(rec, classOf[aliklemencicCrimeReport]))
    val processedCrimes = crimeReports.map(incrementCrimesByWeather)
    processedCrimes.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
