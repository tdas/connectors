package io.delta.core.internal

import io.delta.standalone.data.{RowRecord, RowBatch}
import io.delta.standalone.utils.CloseableIterator

class DeltaRowBatchImpl(rows: CloseableIterator[RowRecord]) extends RowBatch  {
  override def toRowIterator: CloseableIterator[RowRecord] = rows
}
