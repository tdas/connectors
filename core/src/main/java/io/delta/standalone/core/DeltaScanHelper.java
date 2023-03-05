package io.delta.standalone.core;

import java.util.TimeZone;

import io.delta.standalone.data.ColumnarRowBatch;
import io.delta.standalone.types.StructType;
import io.delta.standalone.utils.CloseableIterator;

public interface DeltaScanHelper {
    CloseableIterator<ColumnarRowBatch> readParquetFile(
            String filePath,
            StructType readSchema,
            TimeZone timeZone
    );

    default TimeZone getReadTimeZone() {
        return TimeZone.getDefault();
    };
}
