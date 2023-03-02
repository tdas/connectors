package io.delta.core

// scalastyle:off

import java.util.TimeZone

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.arrow.memory.RootAllocator
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.FunSuite

import io.delta.core.internal.{DeltaLogCoreImpl, DeltaScanCoreImpl}
import io.delta.standalone.core.DeltaScanHelper
import io.delta.standalone.data.{ColumnarRowBatch, RowRecord}
import io.delta.standalone.types._
import io.delta.standalone.utils.CloseableIterator
import io.delta.standalone.DeltaLog
import io.delta.standalone.internal.scan.DeltaScanImpl


class ArrowScanHelper(val hadoopConf: Configuration) extends DeltaScanHelper {
  override def readParquetFile(
      filePath: String,
      readSchema: StructType,
      timeZone: TimeZone): CloseableIterator[ColumnarRowBatch] = {
    ArrowParquetReader.readAsColumnarBatches(filePath, readSchema, ArrowScanHelper.allocator)
  }
}

object ArrowScanHelper {
  val allocator = new RootAllocator
}

class DeltaStandaloneWithCoreSuite extends FunSuite {

  test("scan") {
    GoldenTableUtils.withGoldenTable("data-reader-primitives") { tablePath =>
      val conf = new Configuration()
      val log = DeltaLog.forTable(conf, tablePath)
      val scanHelper = new ArrowScanHelper(conf)
      val snapshot = log.snapshot()
      val scan = snapshot.scan(scanHelper)
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
