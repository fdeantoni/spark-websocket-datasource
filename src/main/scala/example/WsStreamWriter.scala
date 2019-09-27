package example

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming.sources.{PackedRowCommitMessage, PackedRowWriterFactory}
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.sources.v2.writer.{DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.{BinaryType, StringType, StructType}

class WsStreamWriter(schema: StructType, options: DataSourceOptions) extends StreamWriter with WsWriter {

  val url: String = options.get("url").orElse("ws://localhost:9000/test")

  private val started: AtomicBoolean = new AtomicBoolean(false)

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {

    val rows = messages.collect {
      case PackedRowCommitMessage(rs) => rs
    }.flatten

    val valueIdx = schema.fieldIndex("value")
    val valueType = schema.fields(valueIdx).dataType

    rows.foreach { row =>
      valueType match {
        case _: StringType =>
          val json = row.getString(valueIdx)
          send(json.getBytes)
        case _: BinaryType => send(row.getBinary(valueIdx))
        case _ => // do nothing
      }
    }
  }

  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    stopWriter(1001, "Stream aborted")
  }

  override def createWriterFactory(): DataWriterFactory[InternalRow] = {
    synchronized {
      if(started.compareAndSet(false, true)) startWriter()
    }
    PackedRowWriterFactory
  }
}
