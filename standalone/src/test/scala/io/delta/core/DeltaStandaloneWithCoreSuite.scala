package io.delta.core

// scalastyle:off

import java.io.{DataInputStream, File}
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

  val tableNoDVSmallPath = new File(resourcePath, "table-without-dv-small").getCanonicalPath

  test("scan") {
    GoldenTableUtils.withGoldenTable("data-reader-primitives") { tablePath =>
      val conf = new Configuration()
      val log = DeltaLog.forTable(conf, tablePath)
      val snapshot = log.snapshot()
      val scan = snapshot.scan()
      printRows(scan.asInstanceOf[DeltaScanImpl].getRows().asScala)
    }
  }

  test("dv") {
    val conf = new Configuration()
    val log = DeltaLog.forTable(conf, tableDVSmallPath)
    val snapshot = log.snapshot()
    val scan = snapshot.scan()
    printRows(scan.asInstanceOf[DeltaScanImpl].getRows().asScala)
  }

  test("dv large") {
    val conf = new Configuration()
    val log = DeltaLog.forTable(conf, tableDVLargePath)
    val snapshot = log.snapshot()
    val scan = snapshot.scan()
    val valuesSet = scan.asInstanceOf[DeltaScanImpl].getRows().asScala
      .map(_.getInt("value"))
      .toSet
    assert(valuesSet == expectedTable1DataV4.toSet)
  }

  test("no dv") {
    val conf = new Configuration()
    val log = DeltaLog.forTable(conf, tableNoDVSmallPath)
    val snapshot = log.snapshot()
    val scan = snapshot.scan()
    val valuesSet = scan.asInstanceOf[DeltaScanImpl].getRows().asScala
      .map(_.getInt("value"))
      .toSet
    assert(valuesSet == Seq.range(0, 10).toSet)
  }

  test("no dv large") {
    val conf = new Configuration()
    val log = DeltaLog.forTable(conf, tableDVLargePath)
    val snapshot = log.getSnapshotForVersionAsOf(0)
    val scan = snapshot.scan()
    val valuesSet = scan.asInstanceOf[DeltaScanImpl].getRows().asScala
      .map(_.getInt("value"))
      .toSet
    assert(valuesSet == Seq.range(0, 2000).toSet)
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
