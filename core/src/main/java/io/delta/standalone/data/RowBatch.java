package io.delta.standalone.data;

import io.delta.standalone.utils.CloseableIterator;

public interface RowBatch {
    CloseableIterator<RowRecord> toRowIterator();
}
