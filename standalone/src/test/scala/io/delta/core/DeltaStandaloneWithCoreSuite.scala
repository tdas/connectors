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
import io.delta.core.internal.utils.CloseableIteratorScala._
import io.delta.core.internal.utils.CloseableIteratorScalaImpl
import io.delta.standalone.core.{DeltaScanHelper, LogReplayHelper}
import io.delta.standalone.data.{ColumnarRowBatch, RowRecord}
import io.delta.standalone.expressions.Expression
import io.delta.standalone.types._
import io.delta.standalone.utils.CloseableIterator
import io.delta.standalone.DeltaLog
import io.delta.storage.{LocalLogStore, LogStore}


class TestLogReplayHelper(
    val logStore: LogStore,
    val hadoopConf: Configuration) extends LogReplayHelper {
  def this(hadoopConf: Configuration) = this(new LocalLogStore(hadoopConf), hadoopConf)

  val allocator = new RootAllocator

  override def listLogFiles(dir: String): CloseableIterator[String] = {
    logStore.listFrom(new Path(dir), hadoopConf)
      .asScalaClosable.mapAsCloseable(_.getPath.toString).asJava
  }

  override def readParquetFile(
      filePath: String,
      readSchema: StructType,
      pushdownFilters: Array[Expression]): CloseableIterator[RowRecord] = {
    ArrowParquetReader.readAsRows(filePath, readSchema, allocator)
  }

  override def readJsonFile(
    filePath: String,
    readSchema: StructType): CloseableIterator[RowRecord] = {
    val iter = logStore.read(new Path(filePath), hadoopConf)
    new CloseableIteratorScalaImpl[String](iter.asScala, iter.close).mapAsCloseable { jsonText =>
      TestLogReplayHelper.parseJson(jsonText)
    }.asJava
  }
}

object TestLogReplayHelper {
  val mapper = new ObjectMapper
  // mapper.setSerializationInclusion(Include.NON_ABSENT)
  // mapper.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false)

  def parseJson(str: String): RowRecord = {
    new JsonRow(mapper.readTree(str).asInstanceOf[ObjectNode])
  }
}


class TestScanHelper(val hadoopConf: Configuration) extends DeltaScanHelper {
  override def readParquetFile(
      filePath: String,
      readSchema: StructType,
      timeZone: TimeZone): CloseableIterator[ColumnarRowBatch] = {
    ArrowParquetReader.readAsColumnarBatches(filePath, readSchema, TestScanHelper.allocator)
  }
}

object TestScanHelper {
  val allocator = new RootAllocator
}

class DeltaStandaloneWithCoreSuite extends FunSuite {

  test("scan") {
    GoldenTableUtils.withGoldenTable("data-reader-primitives") { tablePath =>
      val conf = new Configuration()
      val log = DeltaLog.forTable(conf, tablePath)
      val scanHelper = new TestScanHelper(conf)
      val snapshot = log.snapshot()
      val scan = snapshot.scan(scanHelper)
      printRows(scan.asInstanceOf[DeltaScanCoreImpl].getRows().asScala)
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
          case _: IntegerType =>  toStr(record.getInt)
          case _: DoubleType =>  toStr(record.getDouble)
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
