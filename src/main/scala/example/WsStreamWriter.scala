package example

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.writer._
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.types.StructType

class WsStreamWriter(schema: StructType, options: DataSourceOptions) extends StreamWriter with WsWriter {

  val url: String = options.get("url").orElse("ws://localhost:9000/test")

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}

  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}

  override def createWriterFactory(): DataWriterFactory[InternalRow] = {
    new WsStreamWriter.Factory(url, schema)
  }
}

object WsStreamWriter {

  class Factory(url: String, schema: StructType) extends DataWriterFactory[InternalRow] {
    override def createDataWriter(partitionId: Int, taskId: Long, epochId: Long): DataWriter[InternalRow] = {
      new WsStreamDataWriter(url, schema)
    }
  }
}
