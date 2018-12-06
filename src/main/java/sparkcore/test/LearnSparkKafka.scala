package sparkcore

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.{SparkConf, SparkContext}
import sparkcore.homework.Constants

object LearnSparkKafka {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("LearnSparkKafka")
    //        conf.setMaster("local[3]")

    val sc = new SparkContext(conf)
    val textFileRDD = sc.textFile(String.format(Constants.hdfs_path, "国内航班数据500条.csv"))
    val mapRDD = textFileRDD.map(line => line.toUpperCase())
    mapRDD.foreachPartition(partition => {
      val props = new Properties()
      props.put("bootstrap.servers", "ghl01:9092")
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      val producer = new KafkaProducer[String, String](props)
      partition.foreach(line => {
        val message = new ProducerRecord[String, String]("ghlTopic", null, line)
        producer.send(message)
      })
      producer.close()
    })

  }
}
