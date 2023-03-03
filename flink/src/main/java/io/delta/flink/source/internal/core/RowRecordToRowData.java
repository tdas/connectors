package io.delta.flink.source.internal.core;

import java.math.BigDecimal;

import io.delta.standalone.data.RowRecord;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;
import org.apache.commons.lang.NotImplementedException;
import org.apache.flink.table.data.*;
import org.apache.flink.types.RowKind;

import static org.apache.flink.types.RowKind.INSERT;

public class RowRecordToRowData implements RowData {

    private RowKind rowKind;
    private final RowRecord impl;
    private final StructField[] schemaFields;

    public RowRecordToRowData(RowRecord impl, StructType schema) {
        this.impl = impl;
        this.schemaFields = schema.getFields();
        this.rowKind = INSERT;
    }

    private String posToFieldName(int pos) {
        if (pos >= schemaFields.length) {
            throw new RuntimeException(
                String.format("Index %s is out of bounds. Schema length: %s", pos, schemaFields.length)
            );
        }
        return schemaFields[pos].getName();
    }

    /**
     * Returns the number of fields in this row.
     */
    @Override
    public int getArity() {
        return schemaFields.length;
    }

    /**
     * Returns the kind of change that this row describes in a changelog.
     */
    @Override
    public RowKind getRowKind() {
        return rowKind;
    }

    @Override
    public void setRowKind(RowKind kind) {
        this.rowKind = kind;
    }

    @Override
    public boolean isNullAt(int pos) {
        return impl.isNullAt(posToFieldName(pos));
    }

    @Override
    public boolean getBoolean(int pos) {
        return impl.getBoolean(posToFieldName(pos));
    }

    @Override
    public byte getByte(int pos) {
        return impl.getByte(posToFieldName(pos));
    }

    @Override
    public short getShort(int pos) {
        return impl.getShort(posToFieldName(pos));
    }

    @Override
    public int getInt(int pos) {
        return impl.getInt(posToFieldName(pos));
    }

    @Override
    public long getLong(int pos) {
        return impl.getLong(posToFieldName(pos));
    }

    @Override
    public float getFloat(int pos) {
        return impl.getFloat(posToFieldName(pos));
    }

    @Override
    public double getDouble(int pos) {
        return impl.getDouble(posToFieldName(pos));
    }

    @Override
    public StringData getString(int pos) {
        return StringData.fromString(impl.getString(posToFieldName(pos)));
    }

    @Override
    public DecimalData getDecimal(int pos, int precision, int scale) {
        final BigDecimal bigDecimal = impl.getBigDecimal(posToFieldName(pos));
        return DecimalData.fromBigDecimal(bigDecimal, bigDecimal.precision(), bigDecimal.scale());
    }

    @Override
    public TimestampData getTimestamp(int pos, int precision) {
        return TimestampData.fromTimestamp(impl.getTimestamp(posToFieldName(pos)));
    }

    @Override
    public <T> RawValueData<T> getRawValue(int pos) {
        throw new NotImplementedException("getRawValue isn't implemented yet");
    }

    @Override
    public byte[] getBinary(int pos) {
        return impl.getBinary(posToFieldName(pos));
    }

    @Override
    public ArrayData getArray(int pos) {
        throw new NotImplementedException("getArray isn't implemented yet");
    }

    @Override
    public MapData getMap(int pos) {
        throw new NotImplementedException("getMap isn't implemented yet");
    }

    @Override
    public RowData getRow(int pos, int numFields) {
        final RowRecord innerRow = impl.getRecord(posToFieldName(pos));
        return new RowRecordToRowData(innerRow, innerRow.getSchema());
    }
}
