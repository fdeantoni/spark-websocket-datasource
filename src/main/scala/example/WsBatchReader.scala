package example

import java.util
import java.util.Optional
import java.util.concurrent.atomic.AtomicBoolean

import javax.annotation.concurrent.GuardedBy
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}
import org.apache.spark.sql.sources.v2.reader.streaming.{MicroBatchReader, Offset}
import org.apache.spark.sql.types.{BinaryType, LongType, StructField, StructType}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class WsBatchReader(options: DataSourceOptions) extends MicroBatchReader with WsReceiver {

  val url: String = options.get("url").orElse("ws://localhost:9000/test")

  @GuardedBy("this")
  private var startOffset: WsOffset = WsOffset.empty

  @GuardedBy("this")
  private var endOffset: WsOffset = WsOffset.empty

  @GuardedBy("this")
  private var eventList: WsBatchReader.List = new WsBatchReader.List(0)

  @GuardedBy("this")
  private val started: AtomicBoolean = new AtomicBoolean(false)

  override def receive(event: WsMessage): Unit = {
    eventList.add(event)
  }

  override def setOffsetRange(start: Optional[Offset], end: Optional[Offset]): Unit = synchronized {
    if(start.isPresent) startOffset = WsOffset(start.get())
    if(end.isPresent) {
      endOffset = WsOffset(end.get())
    } else {
      endOffset = WsOffset(eventList.lastOffset)
    }
  }

  override def getStartOffset: Offset = WsOffset(eventList.committedOffset)

  override def getEndOffset: Offset = WsOffset(eventList.lastOffset)

  override def deserializeOffset(json: String): Offset = WsOffset(json.toLong)

  override def commit(end: Offset): Unit = {
    eventList = eventList.getAtOffset(end.json().toLong)
  }

  override def readSchema(): StructType = StructType(StructField("value", BinaryType) :: StructField("offset", LongType) :: Nil)

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {

    val events = synchronized {
      if(started.compareAndSet(false, true)) startReceiver()
      eventList.getSlice(startOffset.offset, endOffset.offset)
    }

    assert(SparkSession.getActiveSession.isDefined)
    val spark = SparkSession.getActiveSession.get
    val numPartitions = spark.sparkContext.defaultParallelism

    val slices = Array.fill(numPartitions)(new ListBuffer[(WsMessage, Long)])
    events.foreach { case (event, offset) =>
      val hash = math.abs(event.key.hashCode)
      slices( hash % numPartitions).append(event -> offset)
    }

    (0 until numPartitions).map { i =>
      val slice = slices(i)
      new InputPartition[InternalRow] {
        override def createPartitionReader(): InputPartitionReader[InternalRow] =
          new InputPartitionReader[InternalRow] {
            private var currentIdx = -1

            override def next(): Boolean = {
              currentIdx += 1
              currentIdx < slice.size
            }

            override def get(): InternalRow = {
              InternalRow(slice(currentIdx)._1.toJson.getBytes, slice(currentIdx)._2)
            }

            override def close(): Unit = {}
          }
      }
    }.toList.asJava

  }

  override def stop(): Unit = stopReceiver()
}

object WsBatchReader {

  class List(val committedOffset: Long, private var _items: Vector[(WsMessage, Long)] = Vector.empty) extends Logging {
    var lastOffset: Long = _items.lastOption.map(_._2).getOrElse(committedOffset)
    def add(event: WsMessage): Unit = {
      _items :+= ( event -> (lastOffset + 1L))
      lastOffset += 1
    }
    def getAtOffset(offset: Long): List = {
      log.debug(s"getAtOffset($offset)")
      if(offset > lastOffset) {
        new List(lastOffset)
      } else {
        val idx = Math.max(0, offset - committedOffset)
        new List(offset, _items.drop(idx.toInt))
      }
    }
    def getSlice(start: Long, end: Long): Vector[(WsMessage, Long)] = {
      val sliceStart = (start - committedOffset).toInt
      val sliceEnd = (end - committedOffset).toInt
      log.debug(s"getSlice start = $start, end = $end -> committedOffset = $committedOffset, lastOffset = $lastOffset -> sliceStart = $sliceStart, sliceEnd = $sliceEnd")
      _items.slice(sliceStart, sliceEnd)
    }
    override def toString: String = s"committedOffset: $committedOffset, lastOffset: $lastOffset\n" + _items.mkString("\n")
  }
}
