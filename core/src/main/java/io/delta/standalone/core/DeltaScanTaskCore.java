package io.delta.standalone.core;

import java.io.Serializable;
import java.util.Map;

import io.delta.standalone.data.RowBatch;
import io.delta.standalone.types.StructType;
import io.delta.standalone.utils.CloseableIterator;

public interface DeltaScanTaskCore extends Serializable {

    void injectScanHelper(DeltaScanHelper helper);

    CloseableIterator<RowBatch> getDataAsRows();

    String getFilePath();

    StructType getSchema();

    Map<String, String> getPartitionValues();

}
