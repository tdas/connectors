package io.delta.core;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import io.delta.core.internal.ColumnarBatchRow;
import io.delta.standalone.core.RowIndexFilter;
import io.delta.standalone.data.ColumnVector;
import io.delta.standalone.data.ColumnarRowBatch;
import io.delta.standalone.data.RowRecord;
import io.delta.standalone.types.DataType;
import io.delta.standalone.types.StructType;
import io.delta.standalone.utils.CloseableIterator;
import org.apache.arrow.vector.VectorSchemaRoot;

class ArrowColumnarBatch implements ColumnarRowBatch {

    private VectorSchemaRoot vectorSchemaRoot;
    private int numRows; // this is rows in the physical files, doesn't include filtered out DVs
    private List<ArrowColumnVector> fieldVectors;
    private List<String> fieldNames;
    private StructType schema;
    private boolean[] deletionVector;

    public ArrowColumnarBatch(VectorSchemaRoot root, boolean[] deletionVector)
    {
        this(
            root,
            root.getFieldVectors()
                .stream()
                .map(vector -> new ArrowColumnVector(vector))
                .collect(Collectors.toList()),
            root.getSchema().getFields()
                .stream()
                .map(field -> field.getName())
                .collect(Collectors.toList()),
            root.getRowCount(),
            ArrowUtils.fromArrowSchema(root.getSchema()),
            deletionVector
        );
    }

    public ArrowColumnarBatch(
        VectorSchemaRoot vectorSchemaRoot,
        List<ArrowColumnVector> fieldVectors,
        List<String> fieldNames,
        int numRows,
        StructType schema,
        boolean[] deletionVector) {
        this.vectorSchemaRoot = vectorSchemaRoot;
        this.fieldVectors = fieldVectors;
        this.fieldNames = fieldNames;
        this.numRows = numRows;
        this.schema = schema;
        this.deletionVector = deletionVector;
    }

    @Override
    public int getNumRows() {
        return numRows;
    }

    @Override
    public StructType schema() {
        return schema;
    }

    @Override
    public ColumnVector getColumnVector(String columnName) {
        int index = fieldNames.indexOf(columnName);
        if (index >= 0) {
            return fieldVectors.get(index);
        } else {
            throw new IllegalArgumentException(
                "Column vector not found for field '"  + columnName + "'");
        }
    }

    @Override
    public ColumnarRowBatch addColumnWithSingleValue(String columnName, DataType datatype, Object value) {
        return null;
    }

    @Override
    public void close() {
        vectorSchemaRoot.close();
    }

    public CloseableIterator<RowRecord> getRows() {

        ColumnarRowBatch batch = this;

        return new CloseableIterator<RowRecord>() {

            int rowId = 0;
            int maxRowId = getNumRows();

            @Override
            public boolean hasNext() {
                // todo: double check this logic
                if (rowId == maxRowId) {
                    return false;
                } else if (!deletionVector[rowId]) {
                    return true;
                } else {
                    while (rowId < maxRowId && deletionVector[rowId]) {
                        rowId += 1;
                    }
                    return hasNext();
                }
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

