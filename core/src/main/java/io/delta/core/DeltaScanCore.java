
package io.delta.core;

import io.delta.core.utils.CloseableIterator;

public interface DeltaScanCore {
    CloseableIterator<DeltaScanSplitCore> getSplits();
}
