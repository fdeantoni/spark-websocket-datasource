package example

import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit

import okhttp3._
import okio.ByteString
import org.apache.spark.internal.Logging

//noinspection DuplicatedCode
trait WsWriter extends Logging {

  def url: String

  var socket: Option[WebSocket] = None

  protected def startWriter(): Unit = synchronized {

    val client = new OkHttpClient.Builder()
      .readTimeout(0,  TimeUnit.MILLISECONDS)
      .build()

    val request = new Request.Builder()
      .url(url)
      .build()

    client.newWebSocket(request, new WebSocketListener {

      override def onOpen(webSocket: WebSocket, response: Response): Unit = {
        log.debug("Opened websocket connection...")
        socket = Some(webSocket)
      }

      override def onClosed(webSocket: WebSocket, code: Int, reason: String): Unit = {
        log.info(s"Websocket closed: $reason ($code) ")
        if(socket.isDefined) {
          log.warn("Attempting to reconnect in 1s...")
          Thread.sleep(1000)
          startWriter()
        }
      }

      override def onFailure(webSocket: WebSocket, t: Throwable, response: Response): Unit = {
        log.warn(s"Websocket failed: $response\n${t.getMessage}", t)
        if(socket.isDefined) {
          log.warn("Attempting to reconnect in 1s...")
          Thread.sleep(1000)
          startWriter()
        }
      }
    })
  }

  def send(bytes: Array[Byte]): Unit = {
    socket.foreach { ws =>
      val buffer = ByteBuffer.wrap(bytes)
      if(buffer.hasArray) ws.send(ByteString.of(buffer))
    }
  }

  def send(request: WsMessage): Unit = {
    send(request.toJson.getBytes())
  }

  protected def stopWriter(code: Int = 1000, reason: String = "Stream stopped."): Unit = {
    socket.foreach(_.close(code, reason))
    socket = None
  }

}
