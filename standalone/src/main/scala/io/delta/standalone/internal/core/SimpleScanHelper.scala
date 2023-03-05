package io.delta.standalone.internal.core

import java.io.{DataInputStream, File}
import java.util.TimeZone

import org.apache.arrow.memory.RootAllocator
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import io.delta.standalone.core.{DeltaScanHelper, RowIndexFilter}
import io.delta.standalone.data.ColumnarRowBatch
import io.delta.standalone.types.StructType
import io.delta.standalone.utils.CloseableIterator

// scalastyle:off println
// NOTE: this cannot take in a val hadoopConf: Configuration, Configuration is not serializable
class SimpleScanHelper() extends DeltaScanHelper with Serializable {
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
    ArrowParquetReader.readAsColumnarBatches(
      filePath, readSchema, SimpleScanHelper.allocator, filter)
  }

  override def readDeletionVectorFile(filePath: String): DataInputStream = {
    val fs = new Path(filePath).getFileSystem(new Configuration)
    fs.open(new Path(filePath))
  }
}

object SimpleScanHelper {
  val allocator = new RootAllocator
}