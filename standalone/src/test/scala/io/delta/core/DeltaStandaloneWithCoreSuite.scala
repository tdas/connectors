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
  val tableDVSmallPath = new File(resourcePath, "table-with-dv-small").getCanonicalPath


  val tableDVLargePath =  new File(resourcePath, "table-with-dv-large").getCanonicalPath
  // Table at version 0: contains [0, 2000)
  val expectedTable1DataV0 = Seq.range(0, 2000)
  // Table at version 1: removes rows with id = 0, 180, 300, 700, 1800
  val v1Removed = Set(0, 180, 300, 700, 1800)
  val expectedTable1DataV1 = expectedTable1DataV0.filterNot(e => v1Removed.contains(e))
  // Table at version 2: inserts rows with id = 300, 700
  val v2Added = Set(300, 700)
  val expectedTable1DataV2 = expectedTable1DataV1 ++ v2Added
  // Table at version 3: removes rows with id = 300, 250, 350, 900, 1353, 1567, 1800
  val v3Removed = Set(300, 250, 350, 900, 1353, 1567, 1800)
  val expectedTable1DataV3 = expectedTable1DataV2.filterNot(e => v3Removed.contains(e))
  // Table at version 4: inserts rows with id = 900, 1567
  val v4Added = Set(900, 1567)
  val expectedTable1DataV4 = expectedTable1DataV3 ++ v4Added


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

  test("dv large") {
    val conf = new Configuration()
    val log = DeltaLog.forTable(conf, tableDVLargePath)
    val scanHelper = new ArrowScanHelper(conf)
    val snapshot = log.snapshot()
    val scan = snapshot.scan(scanHelper)
    val valuesSet = scan.asInstanceOf[DeltaScanImpl].getRows().asScala
      .map(_.getInt("value"))
      .toSet
    assert(valuesSet == expectedTable1DataV4.toSet)
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
