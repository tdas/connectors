package io.delta.standalone.data;

import java.io.IOException;

import io.delta.core.internal.ColumnarBatchRow;
import io.delta.standalone.types.DataType;
import io.delta.standalone.types.StructType;
import io.delta.standalone.utils.CloseableIterator;

public interface ColumnarRowBatch extends AutoCloseable {

    int getNumRows();

    StructType schema();

    ColumnVector getColumnVector(String columnName);

    ColumnarRowBatch addColumnWithSingleValue(String columnName, DataType datatype, Object value);

    void close();

    default CloseableIterator<RowRecord> getRows() {

        ColumnarRowBatch batch = this;

        return new CloseableIterator<RowRecord>() {

            int rowId = 0;
            int maxRowId = getNumRows();

            @Override
            public boolean hasNext() {
                return rowId < maxRowId;
            }

            @Override
            public RowRecord next() {
                RowRecord row = new ColumnarBatchRow(batch, rowId);
                rowId += 1;
                return row;
            }

            @Override
            public void close() throws IOException { }
        };
    }
}

