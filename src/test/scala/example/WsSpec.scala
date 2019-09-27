package example

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

class WsSpec(val config: Config) extends TestKit(ActorSystem("Test", config)) with ImplicitSender with WordSpecLike with Matchers with BeforeAndAfterAll {

  def this() = this(ConfigFactory.parseString(
    """
      |akka {
      |  loglevel = "DEBUG"
      |  loggers = ["akka.event.slf4j.Slf4jLogger"]
      |}
      |""".stripMargin))

  class TestConfig {
    val spark: SparkConf = new SparkConf()
      .setAppName("value-test")
      .setMaster("local[*]")
      .set("spark.ui.enabled", "false")
  }

  var server: TestWsServer = _
  var session: SparkSession = _

  override def beforeAll(): Unit = {
    server = new TestWsServer()(system)
    session = SparkSession.builder().config(new TestConfig().spark).getOrCreate()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    println("********************  SHUTDOWN  *******************")
    if(session != null) session.stop()
    shutdown()
    super.afterAll()
  }

  "Spark Websocket receiver" should {

    "receive messages as batch" in {

      val spark = session

      val source = spark.readStream
        .format("ws")
        .option("url", server.url)
        .load()

      val stream = source.writeStream
        .format("memory")
        .queryName("messages")
        .start()

      Thread.sleep(10000)

      session.sql("select * from messages").show(10, truncate = false)
      stream.stop()
    }
  }




}
