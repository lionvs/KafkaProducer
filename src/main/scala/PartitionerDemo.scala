import java.util
import java.util.Properties

import org.apache.kafka.clients.producer._
import org.apache.kafka.common.Cluster

class MyPartitioner extends Partitioner {
  override def partition(topic: String, key: Any, keyBytes: Array[Byte], value: Any, valueBytes: Array[Byte], cluster: Cluster): Int = {

    val partitionsLength = cluster.availablePartitionsForTopic(topic).size()

    if (key == null) {
      return 0
    }

    val keySize = key.toString().size;

    if (keySize > 5 && partitionsLength > 1) {
      return 1
    }

    0
  }

  override def close(): Unit = {}

  override def configure(configs: util.Map[String, _]): Unit = {}
}

object PartitionerDemo extends App {

  val props = new Properties();
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "MyPartitioner")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  val cb = new Callback {
    override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
      if (exception != null) {
        println("EXCEPTION")
        println(exception)
        return
      }
      println(s"Partition: ${metadata.partition}")
      println(metadata)
    }
  }

  val topic = "PartitionerDemo-input"

  producer.send(new ProducerRecord[String, String](topic, "222222key", "value"), cb)
  producer.send(new ProducerRecord[String, String](topic, "mykey", "value"), cb)

  producer.send(new ProducerRecord[String, String](topic, "value32"), cb)

  producer.close()
}
