import java.util.Properties

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}

object Main extends App {

  val props = new Properties();
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ProducerConfig.ACKS_CONFIG, "all")
  props.put(ProducerConfig.RETRIES_CONFIG, 0)
  props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384)
  props.put(ProducerConfig.LINGER_MS_CONFIG, 1)
  props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  var fut = producer.send(new ProducerRecord[String, String]("test6626", "key", "value"), new Callback {
    override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
      println(metadata)
      println(exception)
    }
  })

  producer.close()
}
