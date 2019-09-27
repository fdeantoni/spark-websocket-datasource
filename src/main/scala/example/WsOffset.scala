package example

import org.apache.spark.sql.execution.streaming.{Offset, SerializedOffset}
import org.apache.spark.sql.sources.v2

case class WsOffset(offset: Long) extends v2.reader.streaming.Offset {
  override def json(): String = offset.toString

  def +(increment: Long) = WsOffset(offset + increment)
  def -(decrement: Long) = WsOffset(offset - decrement)
}

object WsOffset {
  def apply(offset: SerializedOffset): WsOffset = WsOffset(offset.json.toLong)
  def apply(offset: Offset): WsOffset = WsOffset(offset.json().toLong)
  def empty = WsOffset(-1)
}
