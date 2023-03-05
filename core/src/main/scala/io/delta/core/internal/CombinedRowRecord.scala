package io.delta.core.internal

import java.sql.{Date, Timestamp}

import io.delta.standalone.data.RowRecord
import io.delta.standalone.types._

/**
 * Scala implementation of Java interface [[RowParquetRecordJ]].
 *
 * @param innerRow        the internal parquet4s record
 * @param schema          the intended schema for this record
 * @param partitionValues the deserialized partition values of current record
 */
// scalastyle:off println
case class CombinedRowRecord(
  innerRow: RowRecord,
  schema: StructType,
  partitionValues: Map[String, Any]) extends RowRecord {


  ///////////////////////////////////////////////////////////////////////////
  // Public API Methods
  ///////////////////////////////////////////////////////////////////////////

  override def getSchema: StructType = schema

  override def getLength: Int = innerRow.getLength + partitionValues.size

  override def isNullAt(fieldName: String): Boolean = {
    // println(s"Scott > CombinedRowRecord > isNullAt $fieldName")
    if (partitionValues.contains(fieldName)) { // is partition field
      partitionValues(fieldName) == null
    } else {
      // println(s"Scott > CombinedRowRecord > isNullAt >> calling innerRow")
      innerRow.isNullAt(fieldName)
    }
  }

  override def getInt(fieldName: String): Int = getAs[Int](fieldName)(innerRow.getInt)

  override def getLong(fieldName: String): Long = getAs[Long](fieldName)(innerRow.getLong)

  override def getByte(fieldName: String): Byte = getAs[Byte](fieldName)(innerRow.getByte)

  override def getShort(fieldName: String): Short = getAs[Short](fieldName)(innerRow.getShort)

  override def getBoolean(fieldName: String): Boolean =
    getAs[Boolean](fieldName)(innerRow.getBoolean)

  override def getFloat(fieldName: String): Float = getAs[Float](fieldName)(innerRow.getFloat)

  override def getDouble(fieldName: String): Double = getAs[Double](fieldName)(innerRow.getDouble)

  override def getString(fieldName: String): String = getAs[String](fieldName)(innerRow.getString)

  override def getBinary(fieldName: String): Array[Byte] =
    getAs[Array[Byte]](fieldName)(innerRow.getBinary)

  override def getBigDecimal(fieldName: String): java.math.BigDecimal =
    getAs[java.math.BigDecimal](fieldName)(innerRow.getBigDecimal)

  override def getTimestamp(fieldName: String): Timestamp =
    getAs[Timestamp](fieldName)(innerRow.getTimestamp)

  override def getDate(fieldName: String): Date = getAs[Date](fieldName)(innerRow.getDate)

  override def getRecord(fieldName: String): RowRecord =
    getAs[RowRecord](fieldName)(innerRow.getRecord)

  override def getList[T](fieldName: String): java.util.List[T] =
    getAs[java.util.List[T]](fieldName)(innerRow.getList)

  override def getMap[K, V](fieldName: String): java.util.Map[K, V] =
    getAs[java.util.Map[K, V]](fieldName)(innerRow.getMap)

  ///////////////////////////////////////////////////////////////////////////
  // Decoding Helper Methods
  ///////////////////////////////////////////////////////////////////////////


  /**
   * Decodes the parquet data into the desired type [[T]]
   *
   * @param fieldName the field name to lookup
   * @return the data at column with name `fieldName` as type [[T]]
   * @throws IllegalArgumentException if `fieldName` not in this schema
   * @throws NullPointerException     if field, of type [[StructField]], is not `nullable` and null data
   *                                  value read
   * @throws RuntimeException         if unable to decode the type [[T]]
   */
  private def getAs[T](fieldName: String)(getF: String => T): T = {
    val schemaField = schema.get(fieldName)

    // Partition Field
    if (partitionValues.contains(fieldName)) {
      if (partitionValues(fieldName) == null && !schemaField.isNullable) {
        // throw DeltaErrors.nullValueFoundForNonNullSchemaField(fieldName, schema)
        throw new IllegalArgumentException(
          s"nullValueFoundForNonNullSchemaField($fieldName, $schema)")
      }
      return partitionValues(fieldName).asInstanceOf[T]
    }

    // Data Field
    val isFieldNull = innerRow.isNullAt(fieldName)

    if (isFieldNull && !schemaField.isNullable) {
      // DeltaErrors.nullValueFoundForNonNullSchemaField(fieldName, schema)
      throw new IllegalArgumentException(
        s"nullValueFoundForNonNullSchemaField($fieldName, $schema)")
    }

    if (isFieldNull && primitiveTypeNames.contains(schemaField.getDataType.getTypeName)) {
      // throw DeltaErrors.nullValueFoundForPrimitiveTypes(fieldName)
      throw new IllegalArgumentException(s"nullValueFoundForPrimitiveTypes($fieldName)")
    }
    getF(fieldName)
  }

  private val primitiveTypeNames = Set(
    new IntegerType().getTypeName,
    new LongType().getTypeName,
    new ByteType().getTypeName,
    new ShortType().getTypeName,
    new BooleanType().getTypeName,
    new FloatType().getTypeName,
    new DoubleType().getTypeName)
}
