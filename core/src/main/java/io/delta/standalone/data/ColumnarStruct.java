package io.delta.standalone.data;

public interface ColumnarStruct {

    
    boolean isNullAt(String columnName);

    
    boolean getBoolean(String columnName);

    
    byte getByte(String columnName);

    
    short getShort(String columnName);

    
    int getInt(String columnName);

    
    long getLong(String columnName);

    
    float getFloat(String columnName);

    
    double getDouble(String columnName);

    byte[] getBinary(String columnName);
    
    ColumnarStruct getStruct(String columnName);

    String getString(String columnName);

    /*
    Decimal getDecimal(String columnName);
    
    CalendarInterval getInterval(String columnName);
    ColumnarArray getArray(int ordinal) {
        return data.getChild(ordinal).getArray(rowId);
    }

    ColumnarMap getMap(int ordinal) {
        return data.getChild(ordinal).getMap(rowId);
    }
    */
}
