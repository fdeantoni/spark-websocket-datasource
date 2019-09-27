package example

import java.nio.ByteBuffer
import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue, TimeUnit}

import javax.annotation.concurrent.GuardedBy
import okhttp3._
import okio.ByteString
import org.apache.spark.internal.Logging

import scala.util.{Failure, Success, Try}

//noinspection DuplicatedCode
trait WsReceiver extends Logging {

  def url: String

  def capacity: Int = 512

  @GuardedBy("this")
  protected val messageQueue: BlockingQueue[WsMessage] = new ArrayBlockingQueue[WsMessage](capacity)

  @GuardedBy("this")
  var socket: Option[WebSocket] = None

  @GuardedBy("this")
  var worker: Option[Thread] = None

  protected def startReceiver(): Unit = synchronized {

    val client = new OkHttpClient.Builder()
      .readTimeout(0,  TimeUnit.MILLISECONDS)
      .build()

    val request = new Request.Builder()
      .url(url)
      .build()

    def initialMessage(key: String): ByteString = {
      val initial = WsMessage(key, 0)
      val buffer = ByteBuffer.wrap(initial.toJson.getBytes)
      ByteString.of(buffer)
    }

    val ws = client.newWebSocket(request, new WebSocketListener {

      override def onOpen(webSocket: WebSocket, response: Response): Unit = {
        log.debug("Opened websocket connection...")
        // Send out initial messages which we will get echoed back
        webSocket.send(initialMessage("A"))
        webSocket.send(initialMessage("B"))
      }

      override def onClosed(webSocket: WebSocket, code: Int, reason: String): Unit = {
        log.info(s"Websocket closed: $reason ($code) ")
        if(socket.isDefined) {
          log.warn("Attempting to reconnect in 1s...")
          Thread.sleep(1000)
          startReceiver()
        }
      }

      override def onFailure(webSocket: WebSocket, t: Throwable, response: Response): Unit = {
        log.warn(s"Websocket failed: $response\n${t.getMessage}", t)
        if(socket.isDefined) {
          log.warn("Attempting to reconnect in 1s...")
          Thread.sleep(1000)
          startReceiver()
        }
      }

      override def onMessage(webSocket: WebSocket, bytes: ByteString): Unit = {
        Try(WsMessage.fromBytes(bytes.toByteArray)) match {
          case Success(message) => messageQueue.put(message)
          case Failure(exception) => log.warn("Expected WsMessage bug got " + exception)
        }
      }
    })

    socket = Some(ws)

    worker = {
      val thread = new Thread("Queue Worker") {
        setDaemon(true)
        override def run(): Unit = {
          while(socket.isDefined) {
            val event = messageQueue.poll(100, TimeUnit.MILLISECONDS)
            if(event != null) {
              receive(event)
            }
          }
        }
      }
      thread.start()
      Some(thread)
    }
  }

  protected def stopReceiver(): Unit = {
    socket.foreach(_.close(1000, "Stream stopped."))
    socket = None
  }

  def receive(event: WsMessage)

}
