package io.delta.flink.source.internal.core;

import java.io.IOException;

import io.delta.standalone.core.DeltaScanTaskCore;
import io.delta.standalone.data.RowBatch;
import io.delta.standalone.data.RowRecord;
import io.delta.standalone.utils.CloseableIterator;
import javax.annotation.Nullable;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.flink.table.data.RowData;

/**
 * The actual reader that reads the batches of records.
 */
public class DeltaCoreRowDataReader implements BulkFormat.Reader<RowData> {
    private final DeltaScanTaskCore deltaScanTaskCore;
    private final CloseableIterator<RowBatch> iter;

    public DeltaCoreRowDataReader(DeltaScanTaskCore deltaScanTaskCore) {
        this.deltaScanTaskCore = deltaScanTaskCore;
        this.iter = deltaScanTaskCore.getDataAsRows();
    }

    /**
     * Reads one batch. The method should return null when reaching the end of the input.
     * The returned batch will be handed over to the processing threads as one.
     */
    @Nullable
    @Override
    public BulkFormat.RecordIterator<RowData> readBatch() throws IOException {
        /**
         * An iterator over records with their position in the file. The iterator is closeable to
         * support clean resource release and recycling.
         */
        return new BulkFormat.RecordIterator<RowData>() {
            private CloseableIterator<RowRecord> rowBatchRecordsIter = null;

            @Nullable
            @Override
            public RecordAndPosition<RowData> next() {
                if (rowBatchRecordsIter == null) {
                    if (!iter.hasNext()) {
                        return null;
                    } else {
                        final RowBatch rowBatch = iter.next();
                        rowBatchRecordsIter = rowBatch.toRowIterator(); // TODO: synchronized ?
                    }
                }

                if (!rowBatchRecordsIter.hasNext()) return null;

                RowRecord rowRecord = rowBatchRecordsIter.next();

                RowData rowData = new RowRecordToRowData(rowRecord, deltaScanTaskCore.getSchema());

                return new RecordAndPosition<>(rowData, 0L /* offset */, 0L /* recordSkipCount */);
            }

            @Override
            public void releaseBatch() {

            }
        };
    }

    @Override
    public void close() throws IOException {

    }
}
