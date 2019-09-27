package example

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.util.ByteString

import scala.concurrent.duration._

class TestWsActor(out: ActorRef) extends Actor with ActorLogging {

  import context.dispatcher

  def receive: Receive = {
    case bytes: ByteString =>
      log.info("Received from websocket\n" + bytes.utf8String)
      val message = WsMessage.fromJson(bytes.utf8String)
      self ! message
    case message: WsMessage =>
      log.debug(s"Received event, sending back to $out...")
      val updated = message.copy(index = message.index + 1)
      out ! ByteString(updated.toJson.getBytes)
      context.system.scheduler.scheduleOnce(1.second, self, updated)
    case message => log.warning(s"$self received $message but does not do anything with it...")
  }

  override def preStart(): Unit = {
    log.debug("Client connected...")
  }
}

object TestWsActor {
  def props(out: ActorRef) = Props(new TestWsActor(out))
}
