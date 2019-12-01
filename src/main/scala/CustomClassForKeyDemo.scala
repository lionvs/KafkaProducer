import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.util
import java.util.Properties

import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.Serializer

case class InstEmp(instID: String, empID: String)

class InstEmpSerializer extends Serializer[InstEmp] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def serialize(topic: String, data: InstEmp): Array[Byte] = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(topic)
    oos.writeObject("__**__")
    oos.writeObject(data.instID)
    oos.writeObject("--**--")
    oos.writeObject(data.empID)
    oos.close()
    stream.toByteArray
  }

  override def close(): Unit = {}
}

object Main3 extends App {

  val props = new Properties();
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "InstEmpSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[InstEmp, String](props)

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

  val topic = "custom-serialize"

  producer.send(new ProducerRecord[InstEmp, String](topic, InstEmp("instID", "empID"), "value"), cb)

  producer.close()
}
