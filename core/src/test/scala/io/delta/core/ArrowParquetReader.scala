package io.delta.core

import java.util.Optional

import org.apache.arrow.dataset.file.{FileFormat, FileSystemDatasetFactory}
import org.apache.arrow.dataset.jni.NativeMemoryPool
import org.apache.arrow.dataset.scanner.{Scanner, ScanOptions}
import org.apache.arrow.dataset.source.{Dataset, DatasetFactory}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.ArrowReader
import org.scalatest.prop.Configuration

import io.delta.standalone.core.RowIndexFilter
import io.delta.standalone.data.{ColumnarRowBatch, RowRecord}
import io.delta.standalone.types.StructType
import io.delta.standalone.utils.CloseableIterator

object ArrowParquetReader {

  def readAsColumnarBatches(
    filePath: String,
    readSchema: StructType,
    allocator: RootAllocator,
    filter: RowIndexFilter = null // todo: change the default or accomodate for the null
  ): CloseableIterator[ColumnarRowBatch] = {

    // we can't flatMap the iterators because we need to track the number of seen rows in the file
    // to correctly index into the deletion vector

    new CloseableIterator[ColumnarRowBatch] {

      var physicalRowsRead = 0;
      val arrowBatchIter = readAsColumnarArrowBatches(filePath, readSchema, allocator)
      var currentBatchIter: CloseableIterator[SlicedColumnarBatch] = null

      // Assumes that before calling:
      //   - arrowBatchIter.hasNext
      //   - (logical) currentBatchIter is either null or !currentBatchIter.hasNext
      // note: currentBatchIter's base batch is closed automatically in the arrowBatchIter.hasNext
      private def updateCurrent: Unit = {
        val nextBatch = arrowBatchIter.next()
        currentBatchIter = readAsSlicedBatches(nextBatch, physicalRowsRead, filter)
        physicalRowsRead += nextBatch.getNumRows
      }

      override def hasNext: Boolean = {
        if (currentBatchIter != null && currentBatchIter.hasNext) return true
        if (!arrowBatchIter.hasNext) return false
        updateCurrent
        hasNext
      }

      // assumes hasNext is called before
      override def next: ColumnarRowBatch = {
        currentBatchIter.next()
      }

      override def close(): Unit = {
        if (currentBatchIter != null) {
          currentBatchIter.close()
        }
        arrowBatchIter.close()
      }
    }
  }

  private def readAsColumnarArrowBatches(
    filePath: String,
    readSchema: StructType,
    allocator: RootAllocator,
  ): CloseableIterator[ColumnarRowBatch] = {

    val readColumnNames = readSchema.getFields.map(_.getName)
    // testing with multiple arrow batches
    // val options = new ScanOptions(8, Optional.of(readColumnNames))
    // see ArrowColumnarBatch::close; we close a shared resource somewhere here in this scenario
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
              if (nextBatch != null) { nextBatch.close() }
              nextBatch = new ArrowColumnarBatch(reader.getVectorSchemaRoot())
            } else {
              close()
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
            if (nextBatch != null) { nextBatch.close() }
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

  private def readAsSlicedBatches(
    baseBatch: ColumnarRowBatch,
    fileOffset: Int,
    filter: RowIndexFilter = null) : CloseableIterator[SlicedColumnarBatch] = {
    val numRows = baseBatch.getNumRows
    val deletionVector = new Array[Boolean](numRows)
    filter.materializeIntoVector(fileOffset, fileOffset + numRows, deletionVector)

    new CloseableIterator[SlicedColumnarBatch] {

      // after hasNext has been called
      //  - if hasNext = true --> nextOffset points to a non-deleted row
      //  - if hasNext = false --> nextOffset = numRows
      var nextOffset = 0
      var closed = false

      override def hasNext: Boolean = {
        if (closed) return false

        if (nextOffset >= numRows) {
          false
        } else if (!deletionVector(nextOffset)) {
          true
        } else {
          while (nextOffset < numRows && deletionVector(nextOffset)) {
            nextOffset += 1
          }
          hasNext
        }
      }

      // assumes that hasNext is always called before next
      override def next: SlicedColumnarBatch = {
        if (closed) throw new IllegalStateException("already closed")

        val startIndex = nextOffset
        // since we know nextOffset must be a non-deleted row (see invariants above)
        // and we cannot have a batch size of 0
        var batchSize = 1
        while (startIndex + batchSize < numRows && !deletionVector(startIndex + batchSize)) {
          batchSize += 1
        }
        nextOffset += batchSize
        new SlicedColumnarBatch(baseBatch, startIndex, batchSize)
      }

      override def close(): Unit = {
        // todo: we have to do something about the SlicedColumnarBatch.close()
        if (!closed) {
          baseBatch.close()
          closed = true
        }
      }

    }
  }

  def readAsRows(
    filePath: String,
    readSchema: StructType,
    allocator: RootAllocator,
    filter: RowIndexFilter = null // todo: change the default or accomodate for the null
  ): CloseableIterator[RowRecord] = {
    import io.delta.core.internal.utils.CloseableIteratorScala._

    readAsColumnarBatches(filePath, readSchema, allocator, filter)
      .asScalaCloseable
      .flatMapAsCloseable { batch =>
        println(s"batch = $batch")
        val rows = batch.getRows
        println(s"rows = $rows")
        val x = rows.asScalaCloseable
        x
      }.asJava
  }
}
