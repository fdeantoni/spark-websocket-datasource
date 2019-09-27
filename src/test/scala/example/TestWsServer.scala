package example

import akka.actor._
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ws.{BinaryMessage, Message}
import akka.http.scaladsl.server.Directives.{handleWebSocketMessages, path}
import akka.stream.{ActorMaterializer, Materializer, OverflowStrategy}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.util.ByteString
import grizzled.slf4j.Logging

class TestWsServer(port: Int = 9000)(implicit system: ActorSystem) extends Logging {

  implicit val mat = ActorMaterializer()

  val route = path("test") {
    val flow: Flow[Message, Message, _] = {
      Flow[Message].collect {
        case BinaryMessage.Strict(data) => data
      }
        .via(TestWsServer.actorRef[ByteString, ByteString](out => TestWsActor.props(out)))
        .map(output => BinaryMessage.Strict(output))
    }
    handleWebSocketMessages(flow)
  }

  val binding = Http().bindAndHandle(route, interface = "localhost", port = port)
  val url = s"ws://localhost:$port/test"
  logger.info(s"Websocket server running on $url")

}

object TestWsServer {

  /**
   * Copy of Play's ActorFlow object.
   */
  def actorRef[In, Out](props: ActorRef => Props, bufferSize: Int = 16, overflowStrategy: OverflowStrategy = OverflowStrategy.dropNew)(implicit factory: ActorRefFactory, mat: Materializer): Flow[In, Out, _] = {

    val (outActor, publisher) = Source.actorRef[Out](bufferSize, overflowStrategy)
      .toMat(Sink.asPublisher(false))(Keep.both).run()

    Flow.fromSinkAndSource(
      Sink.actorRef(factory.actorOf(Props(new Actor with ActorLogging {
        val flowActor = context.watch(context.actorOf(props(outActor), "flowActor"))

        def receive = {
          case Status.Success(_) | Status.Failure(_) => flowActor ! PoisonPill
          case Terminated(_) => context.stop(self)
          case other =>
            log.debug(s"Received ${other.getClass.getCanonicalName} sending to $flowActor")
            flowActor ! other
        }

        override def supervisorStrategy = OneForOneStrategy() {
          case _ => SupervisorStrategy.Stop
        }
      })), Status.Success(())),
      Source.fromPublisher(publisher)
    )
  }
}
