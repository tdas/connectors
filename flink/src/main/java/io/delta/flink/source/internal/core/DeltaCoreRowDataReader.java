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

import static org.apache.flink.connector.file.src.util.RecordAndPosition.NO_OFFSET;

/**
 * The actual reader that reads the batches of records.
 *
 * TODO something is very wrong here
 */
public class DeltaCoreRowDataReader implements BulkFormat.Reader<RowData> {
    private final DeltaScanTaskCore deltaScanTaskCore;
    private final CloseableIterator<RowBatch> rowBatchIter; // ITERATOR of ITERATOR of ROWS
    private long hash = 0;

    public DeltaCoreRowDataReader(DeltaScanTaskCore deltaScanTaskCore) {
        // System.out.println("Created DeltaCoreRowDataReader >> " + deltaScanTaskCore);
        this.deltaScanTaskCore = deltaScanTaskCore;
        this.rowBatchIter = deltaScanTaskCore.getDataAsRows();
        this.hash = this.hashCode() % 1000;
    }

    /**
     * Reads one batch. The method should return null when reaching the end of the input.
     * The returned batch will be handed over to the processing threads as one.
     *
     * To implement reuse and to save object allocation, consider using a
     * org.apache.flink.connector.file.src.util.Pool and recycle objects into the Pool in the the
     * BulkFormat.RecordIterator.releaseBatch() method
     */
    @Nullable
    @Override
    public BulkFormat.RecordIterator<RowData> readBatch() throws IOException {
        if (!rowBatchIter.hasNext()) {
            // System.out.println("" + hash + " -- Scott > DeltaCoreRowDataReader > readBatch RETURNING NULL");
            return null;
        }

        RowBatch rowBatch = rowBatchIter.next();
        // System.out.println("" + hash + " -- Scott > DeltaCoreRowDataReader > BulkFormat.RecordIterator<RowData> > next :: rowBatchIter.next()");
        final CloseableIterator<RowRecord> recordsIterForCurrentRowBatch = rowBatch.toRowIterator();

        // System.out.println("" + hash + " -- Scott > DeltaCoreRowDataReader > readBatch RETURNING NEW ITERATOR");

        /**
         * An iterator over records with their position in the file. The iterator is closeable to
         * support clean resource release and recycling.
         */
        return new BulkFormat.RecordIterator<RowData>() {
            private long numRecords = 0;

            /**
             * Gets the next record from the file, together with its position.
             * The position information returned with the record point to the record AFTER the
             * returned record, because it defines the point where the reading should resume once
             * the current record is emitted. The position information is put in the source's state
             * when the record is emitted. If a checkpoint is taken directly after the record is
             * emitted, the checkpoint must to describe where to resume the source reading from
             * after that record.
             *
             * Objects returned by this method may be reused by the iterator. By the time that this
             * method is called again, no object returned from the previous call will be referenced
             * any more. That makes it possible to have a single MutableRecordAndPosition object and
             * return the same instance (with updated record and position) on every call.
             */
            @Nullable
            @Override
            public RecordAndPosition<RowData> next() {
                if (!recordsIterForCurrentRowBatch.hasNext()) {
                    // System.out.println("" + hash + " -- Scott > DeltaCoreRowDataReader > recordsIterForCurrentRowBatch has no next");
                    return null;
                }

                RowRecord rowRecord = recordsIterForCurrentRowBatch.next();
                // System.out.println("" + hash + " -- Scott > DeltaCoreRowDataReader > recordsIterForCurrentRowBatch has next " + rowRecord);
                RowData rowData = new RowRecordToRowData(rowRecord, deltaScanTaskCore.getSchema());
                numRecords++;

                // TODO: MutableRecordAndPosition
                return new RecordAndPosition<>(rowData, NO_OFFSET /* offset */, numRecords + 1/* recordSkipCount */);
            }

            @Override
            public void releaseBatch() {
                // System.out.println("" + hash + " -- Scott > DeltaCoreRowDataReader > releaseBatch");
            }
        };
    }

    @Override
    public void close() throws IOException {

    }
}
