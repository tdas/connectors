package io.delta.standalone.internal.core;

import io.delta.standalone.data.ColumnVector;
import io.delta.standalone.data.ColumnarStruct;

class ArrowColumnarStruct implements ColumnarStruct {

    ArrowColumnVector vector;
    int rowId;

    public ArrowColumnarStruct(ArrowColumnVector vector, int rowId) {
        this.vector = vector;
        this.rowId = rowId;
    }

    @Override
    public boolean isNullAt(String fieldName) {
        return getVector(fieldName).isNullAt(rowId);
    }

    @Override
    public boolean getBoolean(String fieldName) {
        return getVector(fieldName).getBoolean(rowId);
    }

    @Override
    public byte getByte(String fieldName) {
        return getVector(fieldName).getByte(rowId);
    }

    @Override
    public short getShort(String fieldName) {
        return getVector(fieldName).getShort(rowId);
    }

    @Override
    public int getInt(String fieldName) {
        return getVector(fieldName).getInt(rowId);
    }

    @Override
    public long getLong(String fieldName) {
        return getVector(fieldName).getLong(rowId);
    }

    @Override
    public float getFloat(String fieldName) {
        return getVector(fieldName).getFloat(rowId);
    }

    @Override
    public double getDouble(String fieldName) {
        return getVector(fieldName).getDouble(rowId);
    }

    @Override
    public byte[] getBinary(String fieldName) {
        return getVector(fieldName).getBinary(rowId);
    }

    @Override
    public ColumnarStruct getStruct(String fieldName) {
        return getVector(fieldName).getStruct(rowId);
    }

    @Override
    public String getString(String fieldName) {
        return getVector(fieldName).getString(rowId);
    }

    private ColumnVector getVector(String fieldName) {
        return vector.getChildVector(fieldName);
    }
}
