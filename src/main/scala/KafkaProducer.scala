import java.util.Properties

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}

object KafkaProducer extends App {

  val props = new Properties();
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  producer.send(new ProducerRecord[String, String]("test6626", "ke22222y", "value"), new Callback {
    override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
      println(metadata)
      println(exception)
    }
  })

  producer.close()
}
