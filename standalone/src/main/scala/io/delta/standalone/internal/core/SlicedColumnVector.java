package io.delta.standalone.internal.core;

import io.delta.standalone.data.ColumnVector;
import io.delta.standalone.data.ColumnarStruct;
import io.delta.standalone.types.DataType;

class SlicedColumnVector implements ColumnVector {

    private ColumnVector vector;
    private int startIndex;
    private int numRows;

    private int translateRowId(int rowId) {
        assert(rowId < numRows); // todo: make this a better error
        return startIndex + rowId;
    }

    public SlicedColumnVector(ColumnVector baseVector, int startIndex, int numRows) {
        this.vector = baseVector;
        this.startIndex = startIndex;
        this.numRows = numRows;
    }

    @Override
    public DataType getDataType() {
        return vector.getDataType();
    }

    @Override
    public void close() {
        return;
        // todo: we need to not close this if it's still being used by other sliced vectors
//        vector.close();
    }

    @Override
    public boolean isNullAt(int rowId) {
        return vector.isNullAt(translateRowId(rowId));
    }

    @Override
    public boolean getBoolean(int rowId) {
        return vector.getBoolean(translateRowId(rowId));
    }

    @Override
    public byte getByte(int rowId) {
        return vector.getByte(translateRowId(rowId));
    }

    @Override
    public short getShort(int rowId) {
        return vector.getShort(translateRowId(rowId));
    }

    @Override
    public int getInt(int rowId) {
        return vector.getInt(translateRowId(rowId));
    }

    @Override
    public long getLong(int rowId) {
        return vector.getLong(translateRowId(rowId));
    }

    @Override
    public float getFloat(int rowId) {
        return vector.getFloat(translateRowId(rowId));
    }

    @Override
    public double getDouble(int rowId) {
        return vector.getDouble(translateRowId(rowId));
    }

    @Override
    public byte[] getBinary(int rowId) {
        return vector.getBinary(translateRowId(rowId));
    }

    @Override
    public String getString(int rowId) {
        return vector.getString(translateRowId(rowId));
    }

    @Override
    public ColumnarStruct getStruct(int rowId) {
        return vector.getStruct(translateRowId(rowId));
    }
}
