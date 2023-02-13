package io.delta.core;

import java.util.TimeZone;

import io.delta.core.data.DeltaRowBatch;
import io.delta.core.types.StructType;
import io.delta.core.utils.CloseableIterator;

public interface DeltaScanHelper {
    CloseableIterator<DeltaRowBatch> readParquetDataFile(
            String filePath,
            StructType schema,
            TimeZone timeZone
    );

    default TimeZone getReadTimeZone() {
        return TimeZone.getDefault();
    };
}
