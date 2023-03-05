package io.delta.flink.source;

import java.io.File;
import java.util.Map;
import java.util.function.Function;

import com.google.common.collect.ImmutableMap;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.collect.ClientAndIterator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.TestLogger;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class SimpleDeltaCoreSourceSuite extends TestLogger {
    @Test
    public void test_table_without_dv() throws Exception {
        printTable(
            "../standalone/src/test/resources/delta/table-without-dv-small/",
            new String[] { "int" }
        );
    }

    @Test
    public void test_table_with_dv() throws Exception {
        printTable(
            "../standalone/src/test/resources/delta/table-with-dv-small/",
            new String[] { "int" }
        );
    }

    private void printTable(String tablePath, String[] columnTypes) throws Exception {
        System.out.println("\n\n\n" +
            "\n------------------------\n" + tablePath + "\n------------------------\n");

        final Configuration hadoopConf = new Configuration();
        final Path path = Path.fromLocalFile(new File(tablePath));
        DeltaSource<RowData> source = DeltaSource.forBoundedRowData(path, hadoopConf).build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 1000));

        DataStream<RowData> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "delta-source");

        ClientAndIterator<RowData> client = DataStreamUtils.collectWithClient(stream, "Bounded Delta Source Test");

        int count = 0;
        while (client.iterator.hasNext()) {
            RowData row = client.iterator.next();
            System.out.print(toString(row, columnTypes));
            count++;
        }
        System.out.println("# rows: " + count);
    }


    private String toString(RowData row, String[] columnTypes) {
        String str = "|";
        for (int i = 0; i < columnTypes.length; i++) {
            str = str + " " + toString(row, columnTypes[i], i) + " |";
        }
        return str;
    }

    private String toString(RowData row, String type, int pos) {
        Map<String, Function<Integer, Object>> dataTypeNameToFunction =
            ImmutableMap.<String, Function<Integer, Object>>builder()
                .put("boolean", row::getBoolean)
                .put("byte", row::getByte)
                .put("int", row::getInt)
                .put("short", row::getShort)
                .put("long", row::getLong)
                .put("float", row::getFloat)
                .put("double", row::getDouble)
                .put("string", row::getString)
                .build();
        if (row.isNullAt(pos)) {
            return "<NULL>";
        } else if (dataTypeNameToFunction.containsKey(type)) {
            return dataTypeNameToFunction.get(type).apply(pos).toString();
        } else {
            return "...";
        }
    }
}
