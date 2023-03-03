package io.delta.core

// scalastyle:off

import java.io.{DataInputStream, File}
import java.util.TimeZone

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.arrow.memory.RootAllocator
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.FunSuite

import io.delta.core.internal.{DeltaLogCoreImpl, DeltaScanCoreImpl}
import io.delta.standalone.core.{DeltaScanHelper, RowIndexFilter}
import io.delta.standalone.data.{ColumnarRowBatch, RowRecord}
import io.delta.standalone.types._
import io.delta.standalone.utils.CloseableIterator
import io.delta.standalone.DeltaLog
import io.delta.standalone.internal.scan.DeltaScanImpl


class ArrowScanHelper(val hadoopConf: Configuration) extends DeltaScanHelper {
  override def readParquetFile(
      filePath: String,
      readSchema: StructType,
      timeZone: TimeZone,
      filter: RowIndexFilter
  ): CloseableIterator[ColumnarRowBatch] = {
//    if (filter != null) {
//      val test = new Array[Boolean](10)
//      filter.materializeIntoVector(0, 10, test)
//      println("dv for first 10 rows:" + test.toSeq)
//    }
    ArrowParquetReader.readAsColumnarBatches(filePath, readSchema, ArrowScanHelper.allocator, filter)
  }

  override def readDeletionVectorFile(filePath: String): DataInputStream = {
    val fs = new Path(filePath).getFileSystem(hadoopConf)
    fs.open(new Path(filePath))
  }
}

object ArrowScanHelper {
  val allocator = new RootAllocator
}

class DeltaStandaloneWithCoreSuite extends FunSuite {

  val resourcePath = new File("../standalone/src/test/resources/delta").getCanonicalFile
  // version 0 contains 0-9
  // version 1 removes rows 0 and 9
  // latest version (1) data = (1, 2, 3, 4, 5, 6, 7, 8)
  val tableDVSmallPath =new File(resourcePath, "table-with-dv-small").getCanonicalPath

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

  test("dv") {
    val conf = new Configuration()
    val log = DeltaLog.forTable(conf, tableDVSmallPath)
    val scanHelper = new ArrowScanHelper(conf)
    val snapshot = log.snapshot()
    val scan = snapshot.scan(scanHelper)
    printRows(scan.asInstanceOf[DeltaScanImpl].getRows().asScala)
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
