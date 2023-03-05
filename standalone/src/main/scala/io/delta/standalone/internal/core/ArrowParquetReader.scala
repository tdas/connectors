package io.delta.standalone.internal.core

import java.util.Optional

import org.apache.arrow.dataset.file.{FileFormat, FileSystemDatasetFactory}
import org.apache.arrow.dataset.jni.NativeMemoryPool
import org.apache.arrow.dataset.scanner.{Scanner, ScanOptions}
import org.apache.arrow.dataset.source.{Dataset, DatasetFactory}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.ArrowReader

import io.delta.standalone.data.{ColumnarRowBatch, RowRecord}
import io.delta.standalone.types.StructType
import io.delta.standalone.utils.CloseableIterator

object ArrowParquetReader {

  def readAsColumnarBatches(
    filePath: String,
    readSchema: StructType,
    allocator: RootAllocator): CloseableIterator[ColumnarRowBatch] = {

    val readColumnNames = readSchema.getFields.map(_.getName)
    val options = new ScanOptions(32768, Optional.of(readColumnNames))

    var datasetFactory: DatasetFactory = null
    var dataset: Dataset = null
    var scanner: Scanner = null
    var reader: ArrowReader = null

    def closeAll(): Unit = {
      if (scanner != null) scanner.close()
      if (dataset != null) dataset.close()
      if (datasetFactory != null) datasetFactory.close()
      if (reader != null) reader.close()
    }

    try {
      datasetFactory = new FileSystemDatasetFactory(
        allocator, NativeMemoryPool.getDefault, FileFormat.PARQUET, filePath)
      dataset = datasetFactory.finish()
      scanner = dataset.newScan(options)
      reader = scanner.scanBatches()

      new CloseableIterator[ColumnarRowBatch] {
        var isNextBatchLoaded = false
        var nextBatch: ArrowColumnarBatch = null
        var closed = false

        override def hasNext: Boolean = {
          if (closed) return false

          if (!isNextBatchLoaded) {
            isNextBatchLoaded = reader.loadNextBatch()
            if (isNextBatchLoaded) {
              if (nextBatch != null) {
                nextBatch.close()
              }
              nextBatch = new ArrowColumnarBatch(reader.getVectorSchemaRoot())
            } else {
              // close()
              // dont close automatically. let the caller close only when caller has decided
              // that it has consumed all the previous batches.
            }
          }
          isNextBatchLoaded
        }

        override def next(): ColumnarRowBatch = {
          if (closed) throw new IllegalStateException("already closed")
          if (!isNextBatchLoaded) throw new IllegalStateException("next batch has not been loaded")
          isNextBatchLoaded = false
          nextBatch
        }

        override def close(): Unit = {
          if (!closed) {
            closeAll()
            if (nextBatch != null) {
              nextBatch.close()
            }
            isNextBatchLoaded = false
            closed = true
          }
        }
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        closeAll()
        throw e
    }
  }

  def readAsRows(
    filePath: String,
    readSchema: StructType,
    allocator: RootAllocator
  ): CloseableIterator[RowRecord] = {
    import io.delta.core.internal.utils.CloseableIteratorScala._

    readAsColumnarBatches(filePath, readSchema, allocator)
      .asScalaCloseable
      .flatMapAsCloseable { batch =>
        batch.getRows.asScalaCloseable
      }.asJava
  }
}
