package io.delta.core.internal

import java.sql.{Date, Timestamp}
import java.util

import io.delta.standalone.data.{ColumnarRowBatch, ColumnVector, RowRecord}
import io.delta.standalone.types.StructType


class ColumnarBatchRow(
    columnarBatch: ColumnarRowBatch,
    rowId: Int
  ) extends RowRecord {
  /**
   * @return the schema for this {@link RowRecord}
   */
  override def getSchema: StructType = columnarBatch.schema()

  /**
   * @return the number of elements in this {@link RowRecord}
   */
  override def getLength: Int = getSchema.length()

  override def isNullAt(fieldName: String): Boolean = getVector(fieldName).isNullAt(rowId)

  override def getInt(fieldName: String): Int = getVector(fieldName).getInt(rowId)

  override def getLong(fieldName: String): Long = getVector(fieldName).getLong(rowId)

  override def getByte(fieldName: String): Byte = getVector(fieldName).getByte(rowId)

  override def getShort(fieldName: String): Short = getVector(fieldName).getShort(rowId)

  override def getBoolean(fieldName: String): Boolean = getVector(fieldName).getBoolean(rowId)

  override def getFloat(fieldName: String): Float = getVector(fieldName).getFloat(rowId)

  override def getDouble(fieldName: String): Double = getVector(fieldName).getDouble(rowId)

  override def getString(fieldName: String): String = getVector(fieldName).getString(rowId)

  override def getBinary(fieldName: String): Array[Byte] = getVector(fieldName).getBinary(rowId)

  override def getRecord(fieldName: String): RowRecord = {
    new ColumnarStructRow(getVector(fieldName).getStruct(rowId),
      getSchema.get(fieldName).getDataType.asInstanceOf[StructType])
  }

  override def getBigDecimal(fieldName: String): java.math.BigDecimal = {
    throw new UnsupportedOperationException("not implemented yet")
  }

  override def getTimestamp(fieldName: String): Timestamp = {
    throw new UnsupportedOperationException("not implemented yet")
  }

  override def getDate(fieldName: String): Date = {
    throw new UnsupportedOperationException("not implemented yet")
  }

  override def getList[T](fieldName: String): util.List[T] = {
    throw new UnsupportedOperationException("not implemented yet")
  }

  override def getMap[K, V](fieldName: String): util.Map[K, V] = {
    throw new UnsupportedOperationException("not implemented yet")
  }

  private def getVector(fieldName: String): ColumnVector = {
    columnarBatch.getColumnVector(fieldName)
  }
}
