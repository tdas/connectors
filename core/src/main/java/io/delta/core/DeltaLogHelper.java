
package io.delta.core;

import io.delta.core.actions.Action;
import io.delta.core.utils.CloseableIterator;

public interface DeltaLogHelper {
    CloseableIterator<Action> readCheckpointFile(String file);

    CloseableIterator<Action> readVersionFile(String file);

    CloseableIterator<String> listLogFiles(String dir);

    default String canonicalizePath(String path) {
        return path;
    };
}
