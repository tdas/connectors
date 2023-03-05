
package io.delta.standalone.core;

import io.delta.standalone.data.RowRecord;
import io.delta.standalone.expressions.Expression;
import io.delta.standalone.types.StructType;
import io.delta.standalone.utils.CloseableIterator;

public interface LogReplayHelper {
    CloseableIterator<RowRecord> readParquetFile(
            String filePath,
            StructType readSchema,
            Expression[] pushdownFilters
    );

    CloseableIterator<RowRecord> readJsonFile(
            String filePath,
            StructType readSchema
    );

    CloseableIterator<String> listLogFiles(String dir);

    default String canonicalizePath(String path) {
        return path;
    };
}
