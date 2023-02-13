package io.delta.standalone;

import io.delta.standalone.data.CloseableIterator;
import io.delta.standalone.data.RowRecord;

public interface ColumnarRowRecords {
    CloseableIterator<RowRecord> toRowIterator();
}
