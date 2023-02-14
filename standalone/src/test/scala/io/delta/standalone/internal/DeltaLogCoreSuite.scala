package io.delta.standalone.internal

import java.util.TimeZone

import com.github.mjakubowski84.parquet4s.{ParquetReader, RowParquetRecord}
import com.github.mjakubowski84.parquet4s.ParquetReader.Options
import io.delta.storage.{LocalLogStore, LogStore}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.FunSuite

import io.delta.core.internal.utils.CloseableIteratorScala._
import io.delta.standalone.actions.Action
import io.delta.standalone.core.{DeltaLogHelper, DeltaScanHelper}
import io.delta.standalone.data.{RowBatch, RowRecord}
import io.delta.standalone.types.StructType
import io.delta.standalone.utils.CloseableIterator

import io.delta.standalone.internal.actions.{Parquet4sSingleActionWrapper, SingleAction}
import io.delta.standalone.internal.data.RowParquetRecordImpl
import io.delta.standalone.internal.util.{ConversionUtils, JsonUtils}

// scalastyle:on

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


}
