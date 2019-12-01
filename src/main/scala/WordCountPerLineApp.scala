import java.time.Duration
import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}


object WordCountPerLineApp extends App {

  val consumerProps = new Properties();
  consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
  consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "group-1")

  val producerProps = new Properties();
  producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer")

  val consumer = new KafkaConsumer(consumerProps)
  val producer = new KafkaProducer[String, Int](producerProps)

  val consumerTopic = "input"
  val producerTopic = "output"

  consumer.subscribe(Collections.singletonList(consumerTopic))

  for (i <- 0 to 3) {
    val messages = consumer.poll(Duration.ofMinutes(1))

    messages.forEach(mes => {
      println(mes.value())

      val records = mes.value().toString
        .split("\\s+")
        .groupBy(s => s)
        .toList
        .map(item => (item._1, item._2.length))

      println(records)

      records.map(item =>
        producer.send(new ProducerRecord[String, Int](producerTopic, item._1, item._2))
      )

    })
    consumer.commitAsync()
  }

  consumer.close()
  producer.close()
}
