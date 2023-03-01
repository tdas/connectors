package io.delta.standalone.core;

import io.delta.standalone.data.RowBatch;
import io.delta.standalone.utils.CloseableIterator;

public interface DeltaScanTaskCore {
    CloseableIterator<RowBatch> getDataAsRows();
}
