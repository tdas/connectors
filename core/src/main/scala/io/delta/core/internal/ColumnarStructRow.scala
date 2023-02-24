package io.delta.core.internal

import java.sql.{Date, Timestamp}
import java.util

import io.delta.standalone.data.{ColumnarStruct, RowRecord}
import io.delta.standalone.types.StructType

class ColumnarStructRow (columnarStruct: ColumnarStruct, schema: StructType) extends RowRecord {
  /**
   * @return the schema for this {@link RowRecord}
   */
  override def getSchema: StructType = schema

  /**
   * @return the number of elements in this {@link RowRecord}
   */
  override def getLength: Int = getSchema.length()

  override def isNullAt(fieldName: String): Boolean = columnarStruct.isNullAt(fieldName)

  override def getInt(fieldName: String): Int = columnarStruct.getInt(fieldName)

  override def getLong(fieldName: String): Long = columnarStruct.getLong(fieldName)

  override def getByte(fieldName: String): Byte = columnarStruct.getByte(fieldName)

  override def getShort(fieldName: String): Short = columnarStruct.getShort(fieldName)

  override def getBoolean(fieldName: String): Boolean = columnarStruct.getBoolean(fieldName)

  override def getFloat(fieldName: String): Float = columnarStruct.getFloat(fieldName)

  override def getDouble(fieldName: String): Double = columnarStruct.getDouble(fieldName)

  override def getString(fieldName: String): String = columnarStruct.getString(fieldName)

  override def getBinary(fieldName: String): Array[Byte] = columnarStruct.getBinary(fieldName)

  override def getBigDecimal(fieldName: String): java.math.BigDecimal = {
    throw new UnsupportedOperationException("not implemented yet")
  }

  override def getTimestamp(fieldName: String): Timestamp = {
    throw new UnsupportedOperationException("not implemented yet")
  }

  override def getDate(fieldName: String): Date = {
    throw new UnsupportedOperationException("not implemented yet")
  }

  override def getRecord(fieldName: String): RowRecord = {
    new ColumnarStructRow(columnarStruct.getStruct(fieldName),
      schema.get(fieldName).getDataType.asInstanceOf[StructType])
  }

  override def getList[T](fieldName: String): util.List[T] = {
    throw new UnsupportedOperationException("not implemented yet")
  }

  override def getMap[K, V](fieldName: String): util.Map[K, V] = {
    throw new UnsupportedOperationException("not implemented yet")
  }
}

