package io.delta.core

import java.util.TimeZone

import io.delta.core.data.{DeltaRow, DeltaRowBatch}
import io.delta.core.types._
import io.delta.core.utils.CloseableIterator
import io.delta.core.utils.CloseableIteratorScala._


class DeltaScanSplitCoreImpl(
    filePath: String,
    filePartitionValues: Map[String, String],
    schema: StructType,
    readTimeZone: TimeZone,
    scanHelper: DeltaScanHelper) extends DeltaScanSplitCore {


  override def getDataAsRows(): CloseableIterator[DeltaRowBatch] = {
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

    new CloseableIterator[DeltaRowBatch] {
      val parquetReadFields =
        schema.getFields.filterNot(f => filePartitionValues.contains(f.getName))
      val iter = scanHelper.readParquetDataFile(
        filePath, new StructType(parquetReadFields), readTimeZone)
      override def hasNext: Boolean = iter.hasNext
      override def next(): DeltaRowBatch = {
        val extendedRows = iter.next().toRowIterator.asScala.mapAsCloseable(row =>
          ExtendedDeltaRow(row, schema, filePartitionValues).asInstanceOf[DeltaRow]
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


