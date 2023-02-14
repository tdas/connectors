
package io.delta.standalone.core;

import io.delta.standalone.utils.CloseableIterator;

public interface DeltaScanCore {
    CloseableIterator<DeltaScanSplitCore> getSplits();
}
