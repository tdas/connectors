package io.delta.core.arrow;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import io.delta.standalone.data.ColumnVector;
import io.delta.standalone.data.ColumnarRowBatch;
import io.delta.standalone.types.DataType;
import io.delta.standalone.types.StructType;
import org.apache.arrow.vector.VectorSchemaRoot;

class ArrowColumnarBatch implements ColumnarRowBatch {

    private VectorSchemaRoot vectorSchemaRoot;
    private int numRows;
    private List<ArrowColumnVector> fieldVectors;
    private List<String> fieldNames;
    private StructType schema;



    public ArrowColumnarBatch(VectorSchemaRoot root)
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
            ArrowUtils.fromArrowSchema(root.getSchema())
        );
    }

    public ArrowColumnarBatch(
        VectorSchemaRoot vectorSchemaRoot,
        List<ArrowColumnVector> fieldVectors,
        List<String> fieldNames,
        int numRows,
        StructType schema) {
        this.vectorSchemaRoot = vectorSchemaRoot;
        this.fieldVectors = fieldVectors;
        this.fieldNames = fieldNames;
        this.numRows = numRows;
        this.schema = schema;
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
}

