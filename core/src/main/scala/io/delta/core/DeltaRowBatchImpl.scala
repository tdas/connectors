package io.delta.core

import io.delta.core.data.{DeltaRow, DeltaRowBatch}
import io.delta.core.utils.CloseableIterator

class DeltaRowBatchImpl(rows: CloseableIterator[DeltaRow]) extends DeltaRowBatch  {
  override def toRowIterator: CloseableIterator[DeltaRow] = rows
}
