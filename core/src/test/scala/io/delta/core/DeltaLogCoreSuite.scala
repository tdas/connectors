package io.delta.core

// scalastyle:off

import java.util.{Optional, TimeZone}

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.arrow.memory.RootAllocator
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.FunSuite

import io.delta.core.internal.{DeltaLogCoreImpl, DeltaScanCoreImpl}
import io.delta.core.internal.utils.{CloseableIteratorScala, CloseableIteratorScalaImpl}
import io.delta.core.internal.utils.CloseableIteratorScala._
import io.delta.standalone.core.{DeltaScanHelper, LogReplayHelper}
import io.delta.standalone.data.{ColumnarRowBatch, ColumnVector, RowBatch, RowRecord}
import io.delta.standalone.expressions.Expression
import io.delta.standalone.types._
import io.delta.standalone.utils.CloseableIterator
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

class DeltaLogCoreSuite extends FunSuite {

  test("json reading") {
    val row = TestLogReplayHelper.parseJson(
      """{
        |  "str": "123",
        |  "int": 123,
        |  "double": 123.1,
        |  "list-int": [1, 2, 3],
        |  "struct": { "str": "456", "int": 456 }
        |}""".stripMargin)
    assert(row.getString("str") == "123")
    assert(row.getInt("int") == 123)
    assert(row.getDouble("double") == 123.1)
    assert(row.getList("list-int").asScala == Seq(1, 2, 3))
    val row2 = row.getRecord("struct")
    assert(row2.getString("str") == "456")
    assert(row2.getInt("int") == 456)
    val map = row.getMap[String, Any]("struct").asScala
    print(map("str").getClass)
    assert(map("str") == "456")
    assert(map("int") == 456)
  }

  test("parquet reading") {
    GoldenTableUtils.withGoldenTable("data-reader-primitives") { tablePath =>
      val allocator = new RootAllocator()
      val filePath =
        "file:" + tablePath + "/part-00000-4f2f0b9f-50b3-4e7b-96a1-e2bb0f246b06-c000.snappy.parquet"

      val readSchema = new StructType()
        .add("as_long", new LongType)
        .add("as_int", new IntegerType)

      ArrowParquetReader.readAsRows(filePath, readSchema, allocator).asScala
        .foreach { row =>
          val as_long =
            if (row.isNullAt("as_long")) "<>"
            else row.getLong("as_long").toString
          val as_int =
            if (row.isNullAt("as_int")) "<>"
            else row.getInt("as_int").toString
          println(s"as_int = $as_int, as_long = $as_long")
      }
    }
  }

  test("scan") {
    GoldenTableUtils.withGoldenTable("data-reader-primitives") { tablePath =>
      val logPath = s"$tablePath/_delta_log"
      val conf = new Configuration()
      val logHelper = new TestLogReplayHelper(conf)
      val scanHelper = new TestScanHelper(conf)

      val log = new DeltaLogCoreImpl(logPath, logHelper)
      val snapshot = log.getLatestSnapshot()
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
