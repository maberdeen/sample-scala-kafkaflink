import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer08}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

import java.io._
import java.util.Properties


object TestConsumer extends App {

  def whatmessage(dumper: PrintWriter, incoming: String) : String = {
    msgCounter += 1
    if ((msgCounter % 100) == 0) {
      print(s"[${msgCounter}]")
    }
    if (msgCounter > messageLimit)
      throw new Exception
    // print(s"-> ${incoming}\n")
    dumper.write(s"${incoming}\n")
    incoming
  }

  var msgCounter = 0
  val messageLimit = 1000

  val topicName: String = "portal_entity_audit"
//  var kafkaCfg = new Kafka("localhost:9092", "localhost:2181", "qa-pat-scala", "earliest")
  val kafkaCfg = new Kafka("qa-kafka1:9092", "qa-zk1:2181", "qa-pat-scala", "latest")
  val props = kafkaCfg.asProperties("myClient")

  val fileout: String = "pat-raw.json"

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  print("...opening file writer")
  val filewriter = new PrintWriter(new File(fileout))

  val stream = env.addSource(
    new FlinkKafkaConsumer08[String](
      topicName, new SimpleStringSchema(), props)
    ).name("TestConsumerStream").map { rec =>
      whatmessage(filewriter, rec)
    }
  try {
    env.execute("TestConsumer")
  } finally {
    print(s"Closing file writer [${msgCounter} lines]")
    filewriter.close
  }
}


case class Kafka(bootstrap: String,
  zookeeper: String, groupId: String,
  offsetReset: String) {

  def asProperties(clientId: String): Properties = {
    val res = new Properties()
    res.setProperty("topic", "win-confirm")
    res.setProperty("bootstrap.servers", bootstrap)
    res.setProperty("zookeeper.connect", zookeeper)
    res.setProperty("group.id", groupId)
    res.setProperty("auto.offset.reset", offsetReset)
    res.setProperty("client.id", clientId)
    res
  }
}
