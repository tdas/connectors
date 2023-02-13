package io.delta.standalone;


import java.io.Serializable;

import io.delta.standalone.data.CloseableIterator;
import io.delta.standalone.data.RowRecord;

public interface DeltaScanSplit extends Serializable {
    CloseableIterator<ColumnarRowRecords> getDataAsColumnarBatches();
    CloseableIterator<RowRecord> getDataAsRows();
}


