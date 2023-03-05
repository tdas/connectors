package io.delta.flink.source.internal.core;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableMap;
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

    // map idx -> Object
    // for each field in schemaFields, get its data type, call impl.get$DataTYpe, store in a map
    // no longer have a reference to the impl
    private final Object[] values;

    public RowRecordToRowData(RowRecord row, StructType schema) {
        this.schemaFields = schema.getFields();
        this.rowKind = INSERT;
        this.values = new Object[schemaFields.length];
        for (int i = 0; i < schemaFields.length; i++) {
            StructField field = schemaFields[i];
            values[i] = getAsObject(row, field.getName(), field.getDataType());
            System.out.println("" + field.getName() + ": " +
                field.getDataType().getTypeName() + " = " + values[i]);
        }
    }

    private Object getAsObject(RowRecord row, String name, DataType dataType) {
        Map<String, Function<String, Object>> dataTypeNameToFunction =
            ImmutableMap.<String, Function<String, Object>>builder()
                .put("boolean", row::getBoolean)
                .put("byte", row::getByte)
                .put("int", row::getInt)
                .put("integer", row::getInt)
                .put("short", row::getShort)
                .put("long", row::getLong)
                .put("float", row::getFloat)
                .put("double", row::getDouble)
                .put("string", row::getString)
                .build();
        if (!row.isNullAt(name) && dataTypeNameToFunction.containsKey(dataType.getTypeName())) {
            return dataTypeNameToFunction.get(dataType.getTypeName()).apply(name);
        } else {
            return null;
        }
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
        return (boolean) values[pos];
    }

    @Override
    public byte getByte(int pos) {
        return (byte) values[pos];
    }

    @Override
    public short getShort(int pos) {
        return (short) values[pos];
    }

    @Override
    public int getInt(int pos) {
        return (int) values[pos];
    }

    @Override
    public long getLong(int pos) { return (long) values[pos]; }

    @Override
    public float getFloat(int pos) {
        return (float) values[pos];
    }

    @Override
    public double getDouble(int pos) {
        return (double) values[pos];
    }

    @Override
    public StringData getString(int pos) {
        return StringData.fromString((String) values[pos]);
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
