package io.delta.standalone.core;

import java.util.TimeZone;

import io.delta.standalone.data.RowBatch;
import io.delta.standalone.types.StructType;
import io.delta.standalone.utils.CloseableIterator;

public interface DeltaScanHelper {
    CloseableIterator<RowBatch> readParquetDataFile(
            String filePath,
            StructType schema,
            TimeZone timeZone
    );

    default TimeZone getReadTimeZone() {
        return TimeZone.getDefault();
    };
}
