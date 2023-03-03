package io.delta.core

import java.util.TimeZone

import org.apache.arrow.memory.RootAllocator
import org.apache.hadoop.conf.Configuration

import io.delta.core.arrow.ArrowParquetReader
import io.delta.standalone.core.DeltaScanHelper
import io.delta.standalone.data.ColumnarRowBatch
import io.delta.standalone.types.StructType
import io.delta.standalone.utils.CloseableIterator

class SimpleScanHelper(val hadoopConf: Configuration) extends DeltaScanHelper {
  override def readParquetFile(
      filePath: String,
      readSchema: StructType,
      timeZone: TimeZone): CloseableIterator[ColumnarRowBatch] = {
    ArrowParquetReader.readAsColumnarBatches(filePath, readSchema, SimpleScanHelper.allocator)
  }
}

object SimpleScanHelper {
  val allocator = new RootAllocator
}