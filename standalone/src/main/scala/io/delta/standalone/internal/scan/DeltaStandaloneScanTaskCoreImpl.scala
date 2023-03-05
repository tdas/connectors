package io.delta.standalone.internal.scan

import java.util
import java.util.TimeZone

import scala.collection.JavaConverters._

import io.delta.core.internal.{CombinedRowRecord, DeltaRowBatchImpl}
import io.delta.core.internal.utils.CloseableIteratorScala._
import io.delta.standalone.core.{DeltaScanHelper, DeltaScanTaskCore}
import io.delta.standalone.data.{RowBatch, RowRecord}
import io.delta.standalone.types._
import io.delta.standalone.utils.CloseableIterator

// scalastyle:off println
class DeltaStandaloneScanTaskCoreImpl(
    filePath: String,
    filePartitionValues: Map[String, String],
    schema: StructType,
    readTimeZone: TimeZone,
    scanHelper: DeltaScanHelper) extends DeltaScanTaskCore {
  override def getFilePath: String = filePath

  override def getSchema: StructType = schema

  override def getPartitionValues: util.Map[String, String] = filePartitionValues.asJava

  override def getDataAsRows(): CloseableIterator[RowBatch] = {

    var decodedPartitionValues: Map[String, Any] = Map()

    if (null != filePartitionValues) {
      filePartitionValues.foreach { case (fieldName, value) =>
        if (value == null) {
          decodedPartitionValues += (fieldName -> null)
        } else {
          val schemaField = schema.get(fieldName)
          if (schemaField != null) {
            val decodedFieldValue = decodePartition(schemaField.getDataType, value)
            decodedPartitionValues += (fieldName -> decodedFieldValue)
          } else {
            throw new IllegalStateException(s"StructField with name $schemaField was null.")
          }
        }
      }
    }

    new CloseableIterator[RowBatch] {
      val parquetReadFields =
        schema.getFields.filterNot(f => filePartitionValues.contains(f.getName))
      val iter = scanHelper.readParquetFile(
        filePath, new StructType(parquetReadFields), readTimeZone)
      override def hasNext: Boolean = iter.hasNext
      override def next(): RowBatch = {
        val extendedRows = iter.next().getRows.asScalaCloseable.mapAsCloseable(row =>
          CombinedRowRecord(row, schema, filePartitionValues).asInstanceOf[RowRecord]
        )
        new DeltaRowBatchImpl(extendedRows.asJava)
      }
      override def close(): Unit = {}
    }
  }

  /**
   * Follows deserialization as specified here
   * https://github.com/delta-io/delta/blob/master/PROTOCOL.md#Partition-Value-Serialization
   */
  private def decodePartition(elemType: DataType, partitionVal: String): Any = {
    elemType match {
      case _: StringType => partitionVal
      case _: TimestampType => java.sql.Timestamp.valueOf(partitionVal)
      case _: DateType => java.sql.Date.valueOf(partitionVal)
      case _: IntegerType => partitionVal.toInt
      case _: LongType => partitionVal.toLong
      case _: ByteType => partitionVal.toByte
      case _: ShortType => partitionVal.toShort
      case _: BooleanType => partitionVal.toBoolean
      case _: FloatType => partitionVal.toFloat
      case _: DoubleType => partitionVal.toDouble
      case _: DecimalType => new java.math.BigDecimal(partitionVal)
      case _: BinaryType => partitionVal.getBytes("UTF-8")
      case _ =>
        throw new RuntimeException(s"Unknown decode type ${elemType.getTypeName}, $partitionVal")
    }
  }
}


