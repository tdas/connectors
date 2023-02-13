package io.delta.core.data;

import io.delta.core.utils.CloseableIterator;

public interface DeltaRowBatch {
    CloseableIterator<DeltaRow> toRowIterator();
}
