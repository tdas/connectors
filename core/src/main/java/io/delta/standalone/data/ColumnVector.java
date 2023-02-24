package io.delta.standalone.data;


import io.delta.standalone.types.DataType;

public interface ColumnVector extends AutoCloseable {

    /**
     * Returns the data type of this column vector.
     */
    DataType getDataType();

    /**
     * Cleans up memory for this column vector. The column vector is not usable after this.
     * <p>
     * This overwrites {@link AutoCloseable#close} to remove the
     * {@code throws} clause, as column vector is in-memory and we don't expect any exception to
     * happen during closing.
     */
    @Override
    void close();

    /**
     * Returns whether the value at {@code rowId} is NULL.
     */
    boolean isNullAt(int rowId);

    /**
     * Returns the boolean type value for {@code rowId}. The return value is undefined and can be
     * anything, if the slot for {@code rowId} is null.
     */
    boolean getBoolean(int rowId);
    /**
     * Returns the byte type value for {@code rowId}. The return value is undefined and can be
     * anything, if the slot for {@code rowId} is null.
     */
    byte getByte(int rowId);

    /**
     * Returns the short type value for {@code rowId}. The return value is undefined and can be
     * anything, if the slot for {@code rowId} is null.
     */
    short getShort(int rowId);

    /**
     * Returns the int type value for {@code rowId}. The return value is undefined and can be
     * anything, if the slot for {@code rowId} is null.
     */
    int getInt(int rowId);

    /**
     * Returns the long type value for {@code rowId}. The return value is undefined and can be
     * anything, if the slot for {@code rowId} is null.
     */
    long getLong(int rowId);

    /**
     * Returns the float type value for {@code rowId}. The return value is undefined and can be
     * anything, if the slot for {@code rowId} is null.
     */
    float getFloat(int rowId);

    /**
     * Returns the double type value for {@code rowId}. The return value is undefined and can be
     * anything, if the slot for {@code rowId} is null.
     */
    double getDouble(int rowId);

    /**
     * Returns the binary type value for {@code rowId}. If the slot for {@code rowId} is null, it
     * should return null.
     */
    byte[] getBinary(int rowId);


    /**
     * Returns the string type value for {@code rowId}. If the slot for {@code rowId} is null, it
     * should return null.
     */
     String getString(int rowId);

     /**
     * Returns the struct type value for {@code rowId}. If the slot for {@code rowId} is null, it
     * should return null.
     */
    ColumnarStruct getStruct(int rowId);

    /*
     * Returns the array type value for {@code rowId}. If the slot for {@code rowId} is null, it
     * should return null.
     * <p>
     * To support array type, implementations must construct an {@link ColumnarArray} and return it in
     * this method. {@link ColumnarArray} requires a {@link ColumnVector} that stores the data of all
     * the elements of all the arrays in this vector, and an offset and length which points to a range
     * in that {@link ColumnVector}, and the range represents the array for rowId. Implementations
     * are free to decide where to put the data vector and offsets and lengths. For example, we can
     * use the first child vector as the data vector, and store offsets and lengths in 2 int arrays in
     * this vector.
     *
    ColumnarArray getArray(int rowId);

    /**
     * Returns the map type value for {@code rowId}. If the slot for {@code rowId} is null, it
     * should return null.
     * <p>
     * In Spark, map type value is basically a key data array and a value data array. A key from the
     * key array with a index and a value from the value array with the same index contribute to
     * an entry of this map type value.
     * <p>
     * To support map type, implementations must construct a {@link ColumnarMap} and return it in
     * this method. {@link ColumnarMap} requires a {@link ColumnVector} that stores the data of all
     * the keys of all the maps in this vector, and another {@link ColumnVector} that stores the data
     * of all the values of all the maps in this vector, and a pair of offset and length which
     * specify the range of the key/value array that belongs to the map type value at rowId.
     *
    ColumnarMap getMap(int ordinal);

    /**
     * Returns the decimal type value for {@code rowId}. If the slot for {@code rowId} is null, it
     * should return null.
     *
    Decimal getDecimal(int rowId, int precision, int scale);

    /**
     * Returns the calendar interval type value for {@code rowId}. If the slot for
     * {@code rowId} is null, it should return null.
     * <p>
     * In Spark, calendar interval type value is basically two integer values representing the number
     * of months and days in this interval, and a long value representing the number of microseconds
     * in this interval. An interval type vector is the same as a struct type vector with 3 fields:
     * {@code months}, {@code days} and {@code microseconds}.
     * <p>
     * To support interval type, implementations must implement {@link #getChild(int)} and define 3
     * child vectors: the first child vector is an int type vector, containing all the month values of
     * all the interval values in this vector. The second child vector is an int type vector,
     * containing all the day values of all the interval values in this vector. The third child vector
     * is a long type vector, containing all the microsecond values of all the interval values in this
     * vector.
     *
    final CalendarInterval getInterval(int rowId) {
        if (isNullAt(rowId)) return null;
        final int months = getChild(0).getInt(rowId);
        final int days = getChild(1).getInt(rowId);
        final long microseconds = getChild(2).getLong(rowId);
        return new CalendarInterval(months, days, microseconds);
    }


    /**
     * Gets boolean type values from {@code [rowId, rowId + count)}. The return values for the null
     * slots are undefined and can be anything.
     *
    default boolean[] getBooleans(int rowId, int count) {
        boolean[] res = new boolean[count];
        for (int i = 0; i < count; i++) {
            res[i] = getBoolean(rowId + i);
        }
        return res;
    }

    /**
     * Gets byte type values from {@code [rowId, rowId + count)}. The return values for the null slots
     * are undefined and can be anything.
     *
    default byte[] getBytes(int rowId, int count) {
        byte[] res = new byte[count];
        for (int i = 0; i < count; i++) {
            res[i] = getByte(rowId + i);
        }
        return res;
    }

    /**
     * Gets short type values from {@code [rowId, rowId + count)}. The return values for the null
     * slots are undefined and can be anything.
     *
    short[] getShorts(int rowId, int count) {
        short[] res = new short[count];
        for (int i = 0; i < count; i++) {
            res[i] = getShort(rowId + i);
        }
        return res;
    }

    /**
     * Gets int type values from {@code [rowId, rowId + count)}. The return values for the null slots
     * are undefined and can be anything.
     *
    default int[] getInts(int rowId, int count) {
        int[] res = new int[count];
        for (int i = 0; i < count; i++) {
            res[i] = getInt(rowId + i);
        }
        return res;
    }

    /**
     * Gets long type values from {@code [rowId, rowId + count)}. The return values for the null slots
     * are undefined and can be anything.
     *
    default long[] getLongs(int rowId, int count) {
        long[] res = new long[count];
        for (int i = 0; i < count; i++) {
            res[i] = getLong(rowId + i);
        }
        return res;
    }

    /**
     * Gets float type values from {@code [rowId, rowId + count)}. The return values for the null
     * slots are undefined and can be anything.
     *
    default float[] getFloats(int rowId, int count) {
        float[] res = new float[count];
        for (int i = 0; i < count; i++) {
            res[i] = getFloat(rowId + i);
        }
        return res;
    }

    /**
     * Gets double type values from {@code [rowId, rowId + count)}. The return values for the null
     * slots are undefined and can be anything.
     *
    default double[] getDoubles(int rowId, int count) {
        double[] res = new double[count];
        for (int i = 0; i < count; i++) {
            res[i] = getDouble(rowId + i);
        }
        return res;
    }


    */
}
