package io.delta.core;

import java.io.IOException;

import io.delta.core.internal.ColumnarBatchRow;
import io.delta.standalone.data.ColumnVector;
import io.delta.standalone.data.ColumnarRowBatch;
import io.delta.standalone.data.RowRecord;
import io.delta.standalone.types.DataType;
import io.delta.standalone.types.StructType;
import io.delta.standalone.utils.CloseableIterator;

public class SlicedColumnarBatch implements ColumnarRowBatch {

    private ColumnarRowBatch batch;
    private int startIndex;
    private int numRows;

    public SlicedColumnarBatch(
        ColumnarRowBatch batch,
        int startIndex,
        int numRows) {
        this.batch = batch;
        this.startIndex = startIndex;
        this.numRows = numRows;
    }

    @Override
    public int getNumRows() {
        return numRows;
    }

    @Override
    public StructType schema() {
        return batch.schema();
    }

    @Override
    public ColumnVector getColumnVector(String columnName) {
        return new SlicedColumnVector(batch.getColumnVector(columnName), startIndex,  numRows);
    }

    @Override
    public ColumnarRowBatch addColumnWithSingleValue(String columnName, DataType datatype, Object value) {
        return new SlicedColumnarBatch(
            batch.addColumnWithSingleValue(columnName, datatype, value),
            startIndex,
            numRows
        );
    }

    @Override
    // todo: we need to make sure we don't close the base batch when others are still using it
    public void close() {
        return;
//        batch.close();
    }

    @Override
    public CloseableIterator<RowRecord> getRows() {

        return new CloseableIterator<RowRecord>() {

            int rowId = 0;
            int maxRowId = getNumRows();

            @Override
            public boolean hasNext() {
                return rowId < maxRowId;
            }

            @Override
            public RowRecord next() {
                RowRecord row = new ColumnarBatchRow(batch, startIndex + rowId);
                rowId += 1;
                return row;
            }

            @Override
            public void close() throws IOException { }
        };
    }
}
