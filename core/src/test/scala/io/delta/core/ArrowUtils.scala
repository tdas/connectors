package io.delta.core

import scala.collection.JavaConverters._

import org.apache.arrow.vector.complex.MapVector
import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, IntervalUnit, TimeUnit}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}

import io.delta.standalone.types._


object ArrowUtils {

  /** Maps data type from Spark to Arrow. NOTE: timeZoneId required for TimestampTypes */
  def toArrowType(dt: DataType, timeZoneId: String): ArrowType = dt match {
    case _: BooleanType => ArrowType.Bool.INSTANCE
    case _: ByteType => new ArrowType.Int(8, true)
    case _: ShortType => new ArrowType.Int(8 * 2, true)
    case _: IntegerType => new ArrowType.Int(8 * 4, true)
    case _: LongType => new ArrowType.Int(8 * 8, true)
    case _: FloatType => new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
    case _: DoubleType => new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
    case _: StringType => ArrowType.Utf8.INSTANCE
    case _: BinaryType => ArrowType.Binary.INSTANCE
    case _: DateType => new ArrowType.Date(DateUnit.DAY)
    case _: TimestampType if timeZoneId == null =>
      throw new IllegalStateException("Missing timezoneId where it is mandatory.")
    case _: TimestampType => new ArrowType.Timestamp(TimeUnit.MICROSECOND, timeZoneId)
    case _: NullType => ArrowType.Null.INSTANCE
    case d: DecimalType => new ArrowType.Decimal(d.getPrecision, d.getScale)
    case _: TimestampType => new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)
    // case _: YearMonthIntervalType => new ArrowType.Interval(IntervalUnit.YEAR_MONTH)
    // case _: DayTimeIntervalType => new ArrowType.Duration(TimeUnit.MICROSECOND)
    case _ =>
      throw new IllegalArgumentException(s"could not convert core data type $dt to arrow type")
  }

  def fromArrowType(dt: ArrowType): DataType = dt match {
    case ArrowType.Bool.INSTANCE => new BooleanType
    case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 8 => new ByteType
    case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 8 * 2 => new ShortType
    case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 8 * 4 => new IntegerType
    case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 8 * 8 => new LongType
    case float: ArrowType.FloatingPoint
      if float.getPrecision() == FloatingPointPrecision.SINGLE => new FloatType
    case float: ArrowType.FloatingPoint
      if float.getPrecision() == FloatingPointPrecision.DOUBLE => new DoubleType
    case ArrowType.Utf8.INSTANCE => new StringType
    case ArrowType.Binary.INSTANCE => new BinaryType
    case ts: ArrowType.Timestamp if ts.getUnit == TimeUnit.MICROSECOND => new TimestampType
    case ArrowType.Null.INSTANCE => new NullType
    case date: ArrowType.Date if date.getUnit == DateUnit.DAY => new DateType
    case d: ArrowType.Decimal => new DecimalType(d.getPrecision, d.getScale)
    case ts: ArrowType.Timestamp if ts.getUnit == TimeUnit.MICROSECOND && ts.getTimezone == null =>
      new TimestampType()
    /*
    case yi: ArrowType.Interval if yi.getUnit == IntervalUnit.YEAR_MONTH => YearMonthIntervalType()
    case di: ArrowType.Duration if di.getUnit == TimeUnit.MICROSECOND => DayTimeIntervalType()
    */
    case _ =>
      throw new IllegalArgumentException(s"could not convert arrow type $dt to core data type")
  }

  /** Maps field from Spark to Arrow. NOTE: timeZoneId required for TimestampType */
  def toArrowField(
    name: String, dt: DataType, nullable: Boolean, timeZoneId: String): Field = {
    dt match {
      case a: ArrayType =>
        val fieldType = new FieldType(nullable, ArrowType.List.INSTANCE, null)
        new Field(name, fieldType,
          Seq(toArrowField("element", a.getElementType, a.containsNull(), timeZoneId)).asJava)
      case s: StructType =>
        val fieldType = new FieldType(nullable, ArrowType.Struct.INSTANCE, null)
        new Field(name, fieldType,
          s.getFields.map { field =>
            toArrowField(field.getName, field.getDataType, field.isNullable, timeZoneId)
          }.toSeq.asJava)
      case m: MapType =>
        val mapType = new FieldType(nullable, new ArrowType.Map(false), null)
        // Note: Map Type struct can not be null, Struct Type key field can not be null
        new Field(name, mapType,
          Seq(toArrowField(MapVector.DATA_VECTOR_NAME,
            new StructType()
              .add(MapVector.KEY_NAME, m.getKeyType, false)
              .add(MapVector.VALUE_NAME, m.getValueType, m.valueContainsNull()),
            nullable = false,
            timeZoneId)).asJava)
      case dataType =>
        val fieldType = new FieldType(nullable, toArrowType(dataType, timeZoneId), null)
        new Field(name, fieldType, Seq.empty[Field].asJava)
    }
  }

  def fromArrowField(field: Field): DataType = {
    field.getType match {
      case _: ArrowType.Map =>
        val elementField = field.getChildren.get(0)
        val keyType = fromArrowField(elementField.getChildren.get(0))
        val valueType = fromArrowField(elementField.getChildren.get(1))
        new MapType(keyType, valueType, elementField.getChildren.get(1).isNullable)
      case ArrowType.List.INSTANCE =>
        val elementField = field.getChildren().get(0)
        val elementType = fromArrowField(elementField)
        new ArrayType(elementType, elementField.isNullable)
      case ArrowType.Struct.INSTANCE =>
        val fields = field.getChildren().asScala.map { child =>
          val dt = fromArrowField(child)
          new StructField(child.getName, dt, child.isNullable)
        }
        new StructType(fields.toArray)
      case arrowType => fromArrowType(arrowType)
    }
  }

  /** Maps schema from Spark to Arrow. NOTE: timeZoneId required for TimestampType in StructType */
  def toArrowSchema(schema: StructType, timeZoneId: String): Schema = {
    new Schema(schema.getFields.map { field =>
      toArrowField(field.getName, field.getDataType, field.isNullable, timeZoneId)
    }.toIterable.asJava)
  }

  def fromArrowSchema(schema: Schema): StructType = {
    new StructType(schema.getFields.asScala.map { field =>
      val dt = fromArrowField(field)
      new StructField(field.getName, dt, field.isNullable)
    }.toArray)
  }
}
