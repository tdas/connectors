package io.delta.flink.source.internal.core;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.TimeZone;

import io.delta.standalone.core.DeltaScanHelper;
import io.delta.standalone.data.ColumnarRowBatch;
import io.delta.standalone.types.StructType;
import io.delta.standalone.utils.CloseableIterator;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.flink.table.data.RowData;

public class ParquetVectorizedReaderScanHelper implements DeltaScanHelper {

    private final BulkFormat.Reader<RowData> parquetVectorizedReader;

    public ParquetVectorizedReaderScanHelper(BulkFormat.Reader<RowData> parquetVectorizedReader) {
        this.parquetVectorizedReader = parquetVectorizedReader;
    }


    @Override
    public CloseableIterator<ColumnarRowBatch> readParquetFile(String filePath, StructType readSchema, TimeZone timeZone) {
        return new CloseableIterator<ColumnarRowBatch>() {
            private BulkFormat.RecordIterator<RowData> iter = null;

            // EMPTY -> unknown if there is or isn't a next element
            // NON-EMPTY -> there is a next element
            // null -> there is no next element
            private Optional<RowData> nextRowData = Optional.empty();


            {
                try {
                    iter = parquetVectorizedReader.readBatch();
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new RuntimeException("could not read batch");
                }
            }

            @Override
            public void close() throws IOException {

            }

            @Override
            public boolean hasNext() {
                if (nextRowData == null) return false;
                if (nextRowData.isPresent()) return true;

                // We aren't sure if there is or isn't a next element

                fetchNext();

                // Now we are sure, either nextRowData is null or it contains the next element

                return nextRowData != null;
            }

            @Override
            public ColumnarRowBatch next() {
                if (!hasNext()) throw new NoSuchElementException();
                RowData rowData = nextRowData.get();

                // TODO: fundamental mismatch here between the underlying reader ... which reads
                // rows in BATCH but then provides an individual-per-row API .... and this API
                // ... which exposes a RowBatch
                // TODO convert rowData to ColumnarRowBatch

                return null;
            }

            private void fetchNext() {
                RecordAndPosition<RowData> next = iter.next();

                if (next == null) {
                    nextRowData = null;
                } else {
                    nextRowData = Optional.of(next.getRecord());
                }
            }
        };
    }
}
