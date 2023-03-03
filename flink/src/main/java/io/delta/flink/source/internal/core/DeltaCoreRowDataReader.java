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
    private final CloseableIterator<RowBatch> rowBatchIter;

    public DeltaCoreRowDataReader(DeltaScanTaskCore deltaScanTaskCore) {
        System.out.println("Created DeltaCoreRowDataReader >> " + deltaScanTaskCore);
        this.deltaScanTaskCore = deltaScanTaskCore;
        this.rowBatchIter = deltaScanTaskCore == null ? null : deltaScanTaskCore.getDataAsRows();
    }

    /**
     * Reads one batch. The method should return null when reaching the end of the input.
     * The returned batch will be handed over to the processing threads as one.
     */
    @Nullable
    @Override
    public BulkFormat.RecordIterator<RowData> readBatch() throws IOException {
        System.out.println("Scott > DeltaCoreRowDataReader > readBatch");
        /**
         * An iterator over records with their position in the file. The iterator is closeable to
         * support clean resource release and recycling.
         */
        return new BulkFormat.RecordIterator<RowData>() {
            {
                System.out.println("Scott > created new BulkFormat.RecordIterator<RowData>()");
            }
            private CloseableIterator<RowRecord> rowBatchRecordsIter = null;
            private int hashCode = -1; // helpful for debugging
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
                if (rowBatchIter == null) {
                    System.out.println("Scott > DeltaCoreRowDataReader > next :: iter is NULL");
                    return null;
                }
                if (rowBatchRecordsIter == null) {
                    if (!rowBatchIter.hasNext()) {
                        System.out.println("Scott > DeltaCoreRowDataReader > rowBatchIter no next");
                        return null;
                    } else {
                        final RowBatch rowBatch = rowBatchIter.next();
                        System.out.println("Scott > DeltaCoreRowDataReader > called ITER.NEXT()");
                        System.out.println("Scott > DeltaCoreRowDataReader > rowBatch " + rowBatch);
                        rowBatchRecordsIter = rowBatch.toRowIterator(); // TODO: synchronized ?
                        System.out.println("Scott > DeltaCoreRowDataReader > rowBatchRecordsIter " +
                            rowBatchRecordsIter);
                        hashCode = rowBatchRecordsIter.hashCode() % 1000;
                        System.out.println("Scott > DeltaCoreRowDataReader > hashCode " + hashCode);
                    }
                }

                if (!rowBatchRecordsIter.hasNext()) {
                    System.out.println("Scott > DeltaCoreRowDataReader > rowBatchRecordsIter no " +
                        "next");
                    rowBatchRecordsIter = null;
                    // go to the next
                    return next();
                }

                RowRecord rowRecord = rowBatchRecordsIter.next();
                System.out.println(hashCode + " - Scott > DeltaCoreRowDataReader > rowRecord " +
                    rowRecord);
                RowData rowData = new RowRecordToRowData(rowRecord, deltaScanTaskCore.getSchema());
                numRecords++;
                System.out.println(hashCode + " - Scott > DeltaCoreRowDataReader > numRecords " +
                    numRecords);

                // TODO: MutableRecordAndPosition
                return new RecordAndPosition<>(rowData, NO_OFFSET /* offset */, numRecords /* recordSkipCount */);
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
