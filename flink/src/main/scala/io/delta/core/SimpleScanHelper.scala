package io.delta.core

import java.util.TimeZone

import org.apache.arrow.memory.RootAllocator
import org.apache.hadoop.conf.Configuration

import io.delta.core.arrow.ArrowParquetReader
import io.delta.standalone.core.DeltaScanHelper
import io.delta.standalone.data.ColumnarRowBatch
import io.delta.standalone.types.StructType
import io.delta.standalone.utils.CloseableIterator

// scalastyle:off println
// NOTE: this cannot take in a val hadoopConf: Configuration, Configuration is not serializable
class SimpleScanHelper() extends DeltaScanHelper with Serializable {
  // println("Scott > Created SimpleScanHelper")

  override def readParquetFile(
      filePath: String,
      readSchema: StructType,
      timeZone: TimeZone): CloseableIterator[ColumnarRowBatch] = {
    // println(s"Scott > SimpleScanHelper > readParquetFile $filePath ${readSchema.getTreeString}")
    ArrowParquetReader.readAsColumnarBatches(filePath, readSchema, SimpleScanHelper.allocator)
  }
}

object SimpleScanHelper {
  val allocator = new RootAllocator
}