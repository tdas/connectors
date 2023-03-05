package io.delta.standalone.types;

import io.delta.standalone.internal.util.DataTypeParser;
import io.delta.standalone.internal.util.SchemaUtils;

public class DataTypeUtils {
    public static boolean isWriteCompatible(StructType oldSchema, StructType newSchema) {
        return SchemaUtils.isWriteCompatible(oldSchema, newSchema);
    }

    public static DataType fromJson(String json) {
        return DataTypeParser.fromJson(json);
    }

    public static String toJson(DataType type) {
        return DataTypeParser.toJson(type);
    }

    public static String toPrettyJson(DataType type) {
        return DataTypeParser.toPrettyJson(type);
    }
}
