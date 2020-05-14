package example

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{BinaryType, StringType, StructType}

abstract class WsRowWriter(schema: StructType) {

  protected def sendRow(row: InternalRow, writer: WsWriter): Unit = {

    val valueIdx = schema.fieldIndex("value")
    val valueType = schema.fields(valueIdx).dataType

    valueType match {
      case _: StringType =>
        val json = row.getString(valueIdx)
        writer.send(json.getBytes)
      case _: BinaryType => writer.send(row.getBinary(valueIdx))
      case _ => throw new IllegalStateException(s"value attribute unsupported type ${valueType.catalogString}")
    }
  }
}
