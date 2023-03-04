package io.delta.flink.source.internal.core;

import java.io.Serializable;
import java.math.BigDecimal;

import io.delta.standalone.data.RowRecord;
import io.delta.standalone.types.DataType;
import io.delta.standalone.types.LongType;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;
import org.apache.commons.lang.NotImplementedException;
import org.apache.flink.table.data.*;
import org.apache.flink.types.RowKind;

import static org.apache.flink.types.RowKind.INSERT;

public class RowRecordToRowData implements RowData, Serializable {

    private RowKind rowKind;
    private final StructField[] schemaFields;
    private final long hash;

    // map idx -> Object
    // for each field in schemaFields, get its data type, call impl.get$DataTYpe, store in a map
    // no longer have a reference to the impl
    private final Object[] values;

    public RowRecordToRowData(RowRecord impl, StructType schema) {
        this.schemaFields = schema.getFields();
        this.rowKind = INSERT;
        this.values = new Object[schemaFields.length];
        this.hash = hashCode() % 10000;
        System.out.println("Trying to create RowRecordToRowData " + this.hash);
        for (int i = 0; i < schemaFields.length; i++) {
            StructField field = schemaFields[i];
            DataType dataType = field.getDataType();

            if (dataType instanceof LongType) {
                values[i] = impl.getLong(field.getName());
                System.out.println(String.format("[RowRecordToRowData -- " + hash + "] -- Scott > Setting %s -> %s", i, values[i]));
                System.out.println("created RowRecordToRowData " + this.hash + "--->" + values[i]);
            } else {
                throw new UnsupportedOperationException("We only support LongType");
            }
        }
        impl = null;
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
        return values[pos] == null;
    }

    @Override
    public boolean getBoolean(int pos) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte getByte(int pos) {
        throw new UnsupportedOperationException();
    }

    @Override
    public short getShort(int pos) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getInt(int pos) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(int pos) {
        System.out.println("[" + this.hash + "] -- getLong --" + (long) values[pos]);
        return (long) values[pos];
    }

    @Override
    public float getFloat(int pos) {
        throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble(int pos) {
        throw new UnsupportedOperationException();
    }

    @Override
    public StringData getString(int pos) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DecimalData getDecimal(int pos, int precision, int scale) {
        throw new UnsupportedOperationException();
    }

    @Override
    public TimestampData getTimestamp(int pos, int precision) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> RawValueData<T> getRawValue(int pos) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getBinary(int pos) {
        throw new UnsupportedOperationException();
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
        throw new UnsupportedOperationException();
    }
}
