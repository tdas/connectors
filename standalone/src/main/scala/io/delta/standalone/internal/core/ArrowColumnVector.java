package io.delta.standalone.internal.core;


import java.math.BigDecimal;
import java.util.List;

import io.delta.standalone.data.ColumnVector;
import io.delta.standalone.data.ColumnarStruct;
import io.delta.standalone.types.DataType;

import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.holders.NullableVarCharHolder;

/**
 * A column vector backed by Apache Arrow.
 */
class ArrowColumnVector implements ColumnVector {

    DataType type;
    ArrowVectorAccessor accessor;

    // These two are only populated and used if this column is of type struct
    ArrowColumnVector[] childFieldVectors;
    List<String> childFieldNames;

    public ArrowColumnVector getChildVector(String fieldName) {
        int index = childFieldNames.indexOf(fieldName);
        if (index >= 0) {
            return childFieldVectors[index];
        } else {
            throw new IllegalArgumentException(
                    "Column vector not found for field '"  + fieldName + "'");
        }
    }

    public ArrowColumnVector(ValueVector vector) {
        this.type = ArrowUtils.fromArrowField(vector.getField());
        initAccessor(vector);
    }

    @Override
    public DataType getDataType() {
        return this.type;
    }

    @Override
    public void close() {
        if (childFieldVectors != null) {
            for (int i = 0; i < childFieldVectors.length; i++) {
                childFieldVectors[i].close();
                childFieldVectors[i] = null;
            }
            childFieldVectors = null;
        }
        System.out.println(accessor.hash + " -- Scott > ArrowColumnVector > accessor CLOSED");
        accessor.close();
    }

    @Override
    public boolean isNullAt(int rowId) {
        return accessor.isNullAt(rowId);
    }

    @Override
    public boolean getBoolean(int rowId) {
        return accessor.getBoolean(rowId);
    }

    @Override
    public byte getByte(int rowId) {
        return accessor.getByte(rowId);
    }

    @Override
    public short getShort(int rowId) {
        return accessor.getShort(rowId);
    }

    @Override
    public int getInt(int rowId) {
        return accessor.getInt(rowId);
    }

    @Override
    public long getLong(int rowId) {
        return accessor.getLong(rowId);
    }

    @Override
    public float getFloat(int rowId) {
        return accessor.getFloat(rowId);
    }

    @Override
    public double getDouble(int rowId) {
        return accessor.getDouble(rowId);
    }

    @Override
    public byte[] getBinary(int rowId) {
        return accessor.getBinary(rowId);
    }

    @Override
    public String getString(int rowId) {
        return accessor.getString(rowId);
    }

    @Override
    public ColumnarStruct getStruct(int rowId)  {
        if (isNullAt(rowId)) return null;
        return new ArrowColumnarStruct(this, rowId);
    }

    /*
    @Override
    public BigDecimal getDecimal(int rowId, int precision, int scale) {
        if (isNullAt(rowId)) return null;
        return accessor.getDecimal(rowId, precision, scale);
    }

    @Override
    public ColumnarArray getArray(int rowId) {
        if (isNullAt(rowId)) return null;
        return accessor.getArray(rowId);
    }

    @Override
    public ColumnarMap getMap(int rowId) {
        if (isNullAt(rowId)) return null;
        return accessor.getMap(rowId);
    }
    */

    private void initAccessor(ValueVector vector) {
        if (vector instanceof BitVector) {
            accessor = new BooleanAccessor((BitVector) vector);
        } else if (vector instanceof TinyIntVector) {
            accessor = new ByteAccessor((TinyIntVector) vector);
        } else if (vector instanceof SmallIntVector) {
            accessor = new ShortAccessor((SmallIntVector) vector);
        } else if (vector instanceof IntVector) {
            accessor = new IntAccessor((IntVector) vector);
        } else if (vector instanceof BigIntVector) {
            accessor = new LongAccessor((BigIntVector) vector);
        } else if (vector instanceof Float4Vector) {
            accessor = new FloatAccessor((Float4Vector) vector);
        } else if (vector instanceof Float8Vector) {
            accessor = new DoubleAccessor((Float8Vector) vector);
        } else if (vector instanceof VarCharVector) {
            accessor = new StringAccessor((VarCharVector) vector);
        } else if (vector instanceof DecimalVector) {
            accessor = new DecimalAccessor((DecimalVector) vector);
        } else if (vector instanceof VarBinaryVector) {
            accessor = new BinaryAccessor((VarBinaryVector) vector);
        } else if (vector instanceof DateDayVector) {
            accessor = new DateAccessor((DateDayVector) vector);
        } else if (vector instanceof TimeStampMicroTZVector) {
            accessor = new TimestampAccessor((TimeStampMicroTZVector) vector);
        } else if (vector instanceof TimeStampMicroVector) {
            accessor = new TimestampNTZAccessor((TimeStampMicroVector) vector);
        } /* else if (vector instanceof MapVector) {
            MapVector mapVector = (MapVector) vector;
            accessor = new MapAccessor(mapVector);
        } else if (vector instanceof ListVector) {
            ListVector listVector = (ListVector) vector;
            accessor = new ArrayAccessor(listVector);
        } */ else if (vector instanceof StructVector) {
            StructVector structVector = (StructVector) vector;
            accessor = new StructAccessor(structVector); // this should not be used
            childFieldNames = structVector.getChildFieldNames();
            childFieldVectors = new ArrowColumnVector[structVector.size()];
            for (int i = 0; i < childFieldVectors.length; ++i) {
                childFieldVectors[i] = new ArrowColumnVector(structVector.getVectorById(i));
            }
        } else if (vector instanceof NullVector) {
            accessor = new NullAccessor((NullVector) vector);
        } else if (vector instanceof IntervalYearVector) {
            accessor = new IntervalYearAccessor((IntervalYearVector) vector);
        } else if (vector instanceof DurationVector) {
            accessor = new DurationAccessor((DurationVector) vector);
        } else {
            throw new UnsupportedOperationException(
                "could not get accessor for " + vector.getClass().getCanonicalName());
        }
    }

    abstract static class ArrowVectorAccessor {

        final public ValueVector vector;
        public final long hash;

        ArrowVectorAccessor(ValueVector vector) {
            this.vector = vector;
            this.hash = hashCode() % 1000;
        }

        final boolean isNullAt(int rowId) {
            return vector.isNull(rowId);
        }

        final int getNullCount() {
            return vector.getNullCount();
        }

        final void close() {
            vector.close();
        }

        boolean getBoolean(int rowId) {
            throw new UnsupportedOperationException();
        }

        byte getByte(int rowId) {
            throw new UnsupportedOperationException();
        }

        short getShort(int rowId) {
            throw new UnsupportedOperationException();
        }

        int getInt(int rowId) {
            throw new UnsupportedOperationException("in " + this.getClass().getCanonicalName());
        }

        long getLong(int rowId) {
            throw new UnsupportedOperationException();
        }

        float getFloat(int rowId) {
            throw new UnsupportedOperationException();
        }

        double getDouble(int rowId) {
            throw new UnsupportedOperationException();
        }

        byte[] getBinary(int rowId) {
            throw new UnsupportedOperationException();
        }

        String getString(int rowId) {
            throw new UnsupportedOperationException();
        }

        BigDecimal getDecimal(int rowId, int precision, int scale) {
            throw new UnsupportedOperationException();
        }

        /*
        ColumnarArray getArray(int rowId) {
            throw new UnsupportedOperationException();
        }

        ColumnarMap getMap(int rowId) {
            throw new UnsupportedOperationException();
        }
        */
    }

    static class BooleanAccessor extends ArrowVectorAccessor {

        private final BitVector accessor;

        BooleanAccessor(BitVector vector) {
            super(vector);
            this.accessor = vector;
        }

        @Override
        final boolean getBoolean(int rowId) {
            return accessor.get(rowId) == 1;
        }
    }

    static class ByteAccessor extends ArrowVectorAccessor {

        private final TinyIntVector accessor;

        ByteAccessor(TinyIntVector vector) {
            super(vector);
            this.accessor = vector;
        }

        @Override
        final byte getByte(int rowId) {
            return accessor.get(rowId);
        }
    }

    static class ShortAccessor extends ArrowVectorAccessor {

        private final SmallIntVector accessor;

        ShortAccessor(SmallIntVector vector) {
            super(vector);
            this.accessor = vector;
        }

        @Override
        final short getShort(int rowId) {
            return accessor.get(rowId);
        }
    }

    static class IntAccessor extends ArrowVectorAccessor {

        private final IntVector accessor;

        IntAccessor(IntVector vector) {
            super(vector);
            this.accessor = vector;
        }

        @Override
        final int getInt(int rowId) {
            return accessor.get(rowId);
        }
    }

    static class LongAccessor extends ArrowVectorAccessor {

        private final BigIntVector accessor;

        LongAccessor(BigIntVector vector) {
            super(vector);
            this.accessor = vector;
        }

        @Override
        final long getLong(int rowId) {
            return accessor.get(rowId);
        }
    }

    static class FloatAccessor extends ArrowVectorAccessor {

        private final Float4Vector accessor;

        FloatAccessor(Float4Vector vector) {
            super(vector);
            this.accessor = vector;
        }

        @Override
        final float getFloat(int rowId) {
            return accessor.get(rowId);
        }
    }

    static class DoubleAccessor extends ArrowVectorAccessor {

        private final Float8Vector accessor;

        DoubleAccessor(Float8Vector vector) {
            super(vector);
            this.accessor = vector;
        }

        @Override
        final double getDouble(int rowId) {
            return accessor.get(rowId);
        }
    }

    static class BinaryAccessor extends ArrowVectorAccessor {

        private final VarBinaryVector accessor;

        BinaryAccessor(VarBinaryVector vector) {
            super(vector);
            this.accessor = vector;
        }

        @Override
        final byte[] getBinary(int rowId) {
            if (accessor.isNull(rowId)) return null;
            else return accessor.getObject(rowId);
        }
    }

    static class DecimalAccessor extends ArrowVectorAccessor {

        private final DecimalVector accessor;

        DecimalAccessor(DecimalVector vector) {
            super(vector);
            this.accessor = vector;
        }

        @Override
        final BigDecimal getDecimal(int rowId, int precision, int scale) {
            if (isNullAt(rowId)) return null;
            return accessor.getObject(rowId);
        }
    }

    static class StringAccessor extends ArrowVectorAccessor {

        private final VarCharVector accessor;
        private final NullableVarCharHolder stringResult = new NullableVarCharHolder();

        StringAccessor(VarCharVector vector) {
            super(vector);
            this.accessor = vector;
        }

        @Override
        final String getString(int rowId) {
            if (accessor.isNull(rowId)) return null;
            else return accessor.getObject(rowId).toString();
        }
    }

    static class DateAccessor extends ArrowVectorAccessor {

        private final DateDayVector accessor;

        DateAccessor(DateDayVector vector) {
            super(vector);
            this.accessor = vector;
        }

        @Override
        final int getInt(int rowId) {
            return accessor.get(rowId);
        }
    }

    static class TimestampAccessor extends ArrowVectorAccessor {

        private final TimeStampMicroTZVector accessor;

        TimestampAccessor(TimeStampMicroTZVector vector) {
            super(vector);
            this.accessor = vector;
        }

        @Override
        final long getLong(int rowId) {
            return accessor.get(rowId);
        }
    }

    static class TimestampNTZAccessor extends ArrowVectorAccessor {

        private final TimeStampMicroVector accessor;

        TimestampNTZAccessor(TimeStampMicroVector vector) {
            super(vector);
            this.accessor = vector;
        }

        @Override
        final long getLong(int rowId) {
            return accessor.get(rowId);
        }
    }

    /*
    static class ArrayAccessor extends ArrowVectorAccessor {

        private final ListVector accessor;
        private final ArrowColumnVector arrayData;

        ArrayAccessor(ListVector vector) {
            super(vector);
            this.accessor = vector;
            this.arrayData = new ArrowColumnVector(vector.getDataVector());
        }

        @Override
        final ColumnarArray getArray(int rowId) {
            int start = accessor.getElementStartIndex(rowId);
            int end = accessor.getElementEndIndex(rowId);
            return new ColumnarArray(arrayData, start, end - start);
        }
    }
    */

    /**
     * Any call to "get" method will throw UnsupportedOperationException.
     *
     * Access struct values in a ArrowColumnVector doesn't use this accessor. Instead, it uses
     * getStruct() method defined in the parent class. Any call to "get" method in this class is a
     * bug in the code.
     *
     */
    static class StructAccessor extends ArrowVectorAccessor {
        StructAccessor(StructVector vector) {
            super(vector);
        }
    }

    /*
    static class MapAccessor extends ArrowVectorAccessor {
        private final MapVector accessor;
        private final ArrowColumnVector keys;
        private final ArrowColumnVector values;

        MapAccessor(MapVector vector) {
            super(vector);
            this.accessor = vector;
            StructVector entries = (StructVector) vector.getDataVector();
            this.keys = new ArrowColumnVector(entries.getChild(MapVector.KEY_NAME));
            this.values = new ArrowColumnVector(entries.getChild(MapVector.VALUE_NAME));
        }

        @Override
        final ColumnarMap getMap(int rowId) {
            int index = rowId * MapVector.OFFSET_WIDTH;
            int offset = accessor.getOffsetBuffer().getInt(index);
            int length = accessor.getInnerValueCountAt(rowId);
            return new ColumnarMap(keys, values, offset, length);
        }
    }

     */

    static class NullAccessor extends ArrowVectorAccessor {

        NullAccessor(NullVector vector) {
            super(vector);
        }
    }

    static class IntervalYearAccessor extends ArrowVectorAccessor {

        private final IntervalYearVector accessor;

        IntervalYearAccessor(IntervalYearVector vector) {
            super(vector);
            this.accessor = vector;
        }

        @Override
        int getInt(int rowId) {
            return accessor.get(rowId);
        }
    }

    static class DurationAccessor extends ArrowVectorAccessor {

        private final DurationVector accessor;

        DurationAccessor(DurationVector vector) {
            super(vector);
            this.accessor = vector;
        }

        @Override
        final long getLong(int rowId) {
            return DurationVector.get(accessor.getDataBuffer(), rowId);
        }
    }
}
