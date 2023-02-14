package io.delta.standalone.internal

// scalastyle:off

import java.util.TimeZone

import scala.collection.convert.ImplicitConversions.`iterator asScala`

import com.github.mjakubowski84.parquet4s.{ParquetReader, RowParquetRecord}
import com.github.mjakubowski84.parquet4s.ParquetReader.Options

import io.delta.storage.{LocalLogStore, LogStore}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.FunSuite

import io.delta.core.internal.utils.CloseableIteratorScala._
import io.delta.core.internal.{DeltaLogCoreImpl, DeltaScanCoreImpl}
import io.delta.standalone.actions.Action
import io.delta.standalone.core.{DeltaLogHelper, DeltaScanHelper}
import io.delta.standalone.data.{RowBatch, RowRecord}
import io.delta.standalone.types._
import io.delta.standalone.utils.CloseableIterator
import io.delta.standalone.internal.actions.{Parquet4sSingleActionWrapper, SingleAction}
import io.delta.standalone.internal.data.RowParquetRecordImpl
import io.delta.standalone.internal.util.{ConversionUtils, GoldenTableUtils, JsonUtils}
import io.delta.standalone.DeltaLog


class TestLogHelper(
    val logStore: LogStore,
    val hadoopConf: Configuration) extends DeltaLogHelper {
  def this(hadoopConf: Configuration) = this(new LocalLogStore(hadoopConf), hadoopConf)
  override def readCheckpointFile(file: String): CloseableIterator[Action] = {

    // TODO: This is cheating. we need to read using the schema provided.
    val iterable = ParquetReader.read[Parquet4sSingleActionWrapper](
      file, ParquetReader.Options(hadoopConf = hadoopConf))
    val iter = iterable.iterator

    new CloseableIterator[Action] {
      override def hasNext: Boolean = iter.hasNext
      override def next(): Action = {
        ConversionUtils.convertAction(iter.next().unwrap.unwrap)
      }
      override def close(): Unit = iterable.close()
    }

  }

  override def readVersionFile(file: String): CloseableIterator[Action] = {
    val iter = logStore.read(new Path(file), hadoopConf)

    new CloseableIterator[Action] {
      override def hasNext: Boolean = iter.hasNext
      override def next(): Action = {
        // TODO: This is cheating. we need to read using the schema provided.
        ConversionUtils.convertAction(JsonUtils.mapper.readValue[SingleAction](iter.next()).unwrap)
      }
      override def close(): Unit = iter.close()
    }
  }

  override def listLogFiles(dir: String): CloseableIterator[String] = {
    import io.delta.core.internal.utils.CloseableIteratorScala._
    logStore.listFrom(new Path(dir), hadoopConf)
      .asScalaClosable.mapAsCloseable(_.getPath.toString).asJava
  }
}


class TestScanHelper(val hadoopConf: Configuration) extends DeltaScanHelper {
  override def readParquetDataFile(
    filePath: String,
    schema: StructType,
    timeZone: TimeZone): CloseableIterator[RowBatch] = {

    val iterable = ParquetReader.read[RowParquetRecord](
      filePath, Options(timeZone = timeZone, hadoopConf = hadoopConf))
    val iterator = iterable.iterator

    val batch = new RowBatch {
      override def toRowIterator: CloseableIterator[RowRecord] = new CloseableIterator[RowRecord] {
        override def hasNext: Boolean = iterator.hasNext

        override def next(): RowRecord = new RowParquetRecordImpl(
          iterator.next(), schema, timeZone, Map.empty)
        override def close(): Unit = iterable.close()
      }
    }
    Iterator(batch).asClosable.asJava
  }
}

class DeltaLogCoreSuite extends FunSuite {

  test("scan") {
    GoldenTableUtils.withGoldenTable("data-reader-primitives") { tablePath =>
      println("======== Expected ========")
      printTable(tablePath)
      println("=" * 40 + "\n\n")
      println("======== Found ========")

      val logPath = s"$tablePath/_delta_log"
      val conf = new Configuration()
      val logHelper = new TestLogHelper(conf)
      val scanHelper = new TestScanHelper(conf)

      val log = new DeltaLogCoreImpl(logPath, logHelper)
      val snapshot = log.getLatestSnapshot()
      val scan = snapshot.scan(scanHelper)

      printRows(scan.asInstanceOf[DeltaScanCoreImpl].getRows())
    }
  }

  def printTable(tablePath: String): Unit = {
    printRows(DeltaLog.forTable(new Configuration(), tablePath).snapshot().open())
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
