package example

import java.util.Optional

import org.apache.spark.internal.Logging
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, MicroBatchReadSupport, StreamWriteSupport}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

class WsStreamingProvider extends DataSourceV2 with MicroBatchReadSupport with StreamWriteSupport with DataSourceRegister with Logging {
  override def createMicroBatchReader(schema: Optional[StructType], checkpointLocation: String, options: DataSourceOptions): MicroBatchReader = {
    new WsBatchReader(options)
  }

  override def createStreamWriter(queryId: String, schema: StructType, mode: OutputMode, options: DataSourceOptions): StreamWriter = {
    new WsStreamWriter(schema, options)
  }

  override def shortName(): String = "ws"
}

