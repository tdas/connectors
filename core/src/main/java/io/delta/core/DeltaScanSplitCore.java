package io.delta.core;

import io.delta.core.data.DeltaRowBatch;
import io.delta.core.utils.CloseableIterator;

public interface DeltaScanSplitCore {
    CloseableIterator<DeltaRowBatch> getDataAsRows();
}
