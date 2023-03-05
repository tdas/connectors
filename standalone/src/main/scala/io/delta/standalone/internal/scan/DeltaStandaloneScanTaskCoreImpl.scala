package io.delta.standalone.internal.scan

import java.io.DataInputStream
import java.util
import java.util.TimeZone

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path

import io.delta.core.internal.{CombinedRowRecord, DeltaRowBatchImpl}
import io.delta.core.internal.utils.CloseableIteratorScala._
import io.delta.standalone.core.{DeltaScanHelper, DeltaScanTaskCore, RowIndexFilter}
import io.delta.standalone.data.{RowBatch, RowRecord}
import io.delta.standalone.internal.actions.DeletionVectorDescriptor
import io.delta.standalone.internal.deletionvectors.{RoaringBitmapArray, StoredDeletionVector}
import io.delta.standalone.types._
import io.delta.standalone.utils.CloseableIterator

class DeltaStandaloneScanTaskCoreImpl(
  tablePath: Path,
  filePath: String,
  filePartitionValues: Map[String, String],
  schema: StructType,
  deletionVector: DeletionVectorDescriptor,
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

    val rowsFilter: RowIndexFilter = if (deletionVector != null) {
      createInstance(deletionVector, Some(tablePath))
    } else {
      new KeepAllRowsFilter
    }

    new CloseableIterator[RowBatch] {
      val parquetReadFields =
        schema.getFields.filterNot(f => filePartitionValues.contains(f.getName))
      val iter = scanHelper.readParquetFile(
        filePath, new StructType(parquetReadFields), readTimeZone, rowsFilter)
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

  def createInstance(
    deletionVector: DeletionVectorDescriptor,
    tablePath: Option[Path]): RowIndexFilter = {
    if (deletionVector.cardinality == 0) {
      // no rows are deleted according to the deletion vector, create a constant row index filter
      // that keeps all rows
      new KeepAllRowsFilter
    } else {
      require(tablePath.nonEmpty, "Table path is required for non-empty deletion vectors")
      val storedBitmap = StoredDeletionVector.create(deletionVector, tablePath.get)
      val generateInputStream: String => DataInputStream = if (deletionVector.isOnDisk) {
        scanHelper.readDeletionVectorFile
      } else {
        _ => null
      }
      val bitmap = storedBitmap.load(generateInputStream)
      new DeletedRowsMarkingFilter(bitmap)
    }
  }
}

final class KeepAllRowsFilter extends RowIndexFilter {
  override def materializeIntoVector(
    start: Long, end: Long, batch: Array[Boolean]): Unit = {
    val batchSize = (end - start).toInt
    var rowId = 0
    while (rowId < batchSize) {
      batch(rowId) = false
      rowId += 1
    }
  }
}

/**
 * Implementation of [[RowIndexFilter]] which checks, for a given row index and deletion vector,
 * whether the row index is present in the deletion vector.If present, the row is marked for
 * skipping.
 * @param bitmap Represents the deletion vector
 */
final class DeletedRowsMarkingFilter(bitmap: RoaringBitmapArray) extends RowIndexFilter {

  override def materializeIntoVector(start: Long, end: Long, batch: Array[Boolean]): Unit = {
    val batchSize = (end - start).toInt
    var rowId = 0
    while (rowId < batchSize) {
      val isContained = bitmap.contains(start + rowId.toLong)
      if (isContained) {
        batch(rowId) = true
      } else {
        batch(rowId) = false
      }
      rowId += 1
    }
  }
}
