package example

import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{read, write}

case class WsMessage(key: String, index: Long) {
  def toJson: String = WsMessage.toJson(this)
}

object WsMessage {
  implicit val formats: Formats = Serialization.formats(NoTypeHints)
  def fromBytes(bytes: Array[Byte]): WsMessage = {
    val string = new String(bytes)
    fromJson(string)
  }
  def fromJson(json: String): WsMessage = {
    read[WsMessage](json)
  }
  def toJson(message: WsMessage): String = write(message)
}
