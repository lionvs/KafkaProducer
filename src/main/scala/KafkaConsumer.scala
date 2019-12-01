import java.time.Duration
import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer._


object KafkaConsumer extends App {

  val props = new Properties();
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  props.put(ConsumerConfig.GROUP_ID_CONFIG, "group-1")

  val consumer = new KafkaConsumer(props)

  val topic = "kafka-consumer"

  consumer.subscribe(Collections.singletonList(topic))

  for( i <- 0 to 3) {
    val messages = consumer.poll(Duration.ofMinutes(1))
    messages.forEach(mes => {
      println(mes.topic())
      println(mes.key())
      println(mes.value())
      println(mes)
    })
    consumer.commitAsync()
  }

  consumer.close()
}
