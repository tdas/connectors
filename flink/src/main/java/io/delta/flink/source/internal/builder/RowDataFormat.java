package io.delta.flink.source.internal.builder;

import java.io.IOException;

import io.delta.flink.source.internal.core.DeltaCoreRowDataReader;
import io.delta.flink.source.internal.core.ParquetVectorizedReaderScanHelper;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import javax.annotation.Nullable;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.formats.parquet.ParquetColumnarRowInputFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetReader;

/**
 * Implementation of {@link DeltaBulkFormat} for {@link RowData} type.
 */
public class RowDataFormat implements DeltaBulkFormat<RowData> {

    private final ParquetColumnarRowInputFormat<DeltaSourceSplit> decoratedInputFormat;

    public RowDataFormat(ParquetColumnarRowInputFormat<DeltaSourceSplit> inputFormat) {
        this.decoratedInputFormat = inputFormat;
    }

    public static RowDataFormatBuilder builder(RowType rowType, Configuration hadoopConfiguration) {
        return new RowDataFormatBuilder(rowType, hadoopConfiguration);
    }

    @Override
    public Reader<RowData> createReader(
            org.apache.flink.configuration.Configuration configuration,
            DeltaSourceSplit deltaSourceSplit) throws IOException {
        // System.out.println("Scott > RowDataFormat :: createReader, split " + deltaSourceSplit.path());
        BulkFormat.Reader<RowData> reader = this.decoratedInputFormat.createReader(configuration, deltaSourceSplit);

        deltaSourceSplit.deltaScanTaskCore.injectScanHelper(new ParquetVectorizedReaderScanHelper(reader));

        return new DeltaCoreRowDataReader(deltaSourceSplit.deltaScanTaskCore);
    }

    @Override
    public Reader<RowData> restoreReader(
            org.apache.flink.configuration.Configuration configuration,
            DeltaSourceSplit deltaSourceSplit) throws IOException {

        return this.decoratedInputFormat.restoreReader(configuration, deltaSourceSplit);
    }

    @Override
    public boolean isSplittable() {
        return this.decoratedInputFormat.isSplittable();
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return this.decoratedInputFormat.getProducedType();
    }
}
