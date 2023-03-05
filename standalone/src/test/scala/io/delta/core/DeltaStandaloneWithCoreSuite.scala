package io.delta.core

// scalastyle:off

import java.util.TimeZone

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.scalatest.FunSuite

import io.delta.standalone.data.RowRecord
import io.delta.standalone.types._
import io.delta.standalone.DeltaLog
import io.delta.standalone.internal.scan.DeltaScanImpl
import io.delta.standalone.internal.util.GoldenTableUtils


class DeltaStandaloneWithCoreSuite extends FunSuite {

  test("scan") {
    GoldenTableUtils.withGoldenTable("data-reader-primitives") { tablePath =>
      val conf = new Configuration()
      val log = DeltaLog.forTable(conf, tablePath)
      val snapshot = log.snapshot()
      val scan = snapshot.scan()
      printRows(scan.asInstanceOf[DeltaScanImpl].getRows().asScala)
    }
  }

  def printRows(iterator: Iterator[RowRecord]): Unit = {
    iterator.foreach { record =>
      println("-" * 20)
      val schemaStr =
        record.getSchema.getFields
          .map(f => s"${f.getName}: ${f.getDataType.getSimpleString}")
          .mkString("[", ", ", "]")
      // println("schema: " + schemaStr)

      val valueStr = record.getSchema.getFields.map { f =>
        def toStr(func: String => _): String = {
          if (record.isNullAt(f.getName)) "<NULL>" else func(f.getName).toString
        }

        val value = f.getDataType match {
          case _: ByteType => toStr(record.getByte)
          case _: IntegerType =>  toStr(record.getInt)
          case _: LongType =>  toStr(record.getLong)
          case _: ShortType => toStr(record.getShort)
          case _: DoubleType =>  toStr(record.getDouble)
          case _: FloatType =>  toStr(record.getFloat)
          case _: BooleanType =>  toStr(record.getBoolean)
          case _: StringType => toStr(record.getString)
          case _ => "..."
        }
        s"${f.getName} = $value"
      }.mkString("[", ", ", "]")

      println(valueStr)
    }
  }
}
