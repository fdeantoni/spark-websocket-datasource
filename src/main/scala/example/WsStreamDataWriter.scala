package example

import org.apache.spark.sql.sources.v2.writer._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

class WsStreamDataWriter(url: String, schema: StructType) extends WsRowWriter(schema) with DataWriter[InternalRow] {

  private lazy val writer = WsStreamDataWriter.getOrCreate(url)

  override def write(record: InternalRow): Unit = {
    sendRow(record, writer)
  }

  override def commit(): WriterCommitMessage = WsStreamDataWriter.CommitMessage

  override def abort(): Unit = {}
}

object WsStreamDataWriter {

  case object CommitMessage extends WriterCommitMessage

  case class CachedStreamWriter(url: String) extends WsWriter {
    def start(): Unit = {
      startWriter()
    }
    def stop(): Unit = {
      stopWriter()
    }
    def abort(): Unit = {
      stopWriter(1001, "Stream aborted")
    }
  }

  private var instance: Option[CachedStreamWriter] = None

  def getOrCreate(url: String): CachedStreamWriter = synchronized {
    instance.getOrElse {
      val s = CachedStreamWriter(url)
      s.start()
      sys.addShutdownHook {
        s.stop()
      }
      instance = Some(s)
      s
    }
  }
}
