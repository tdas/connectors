package io.delta.flink.source;

import java.io.File;

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
    public void test00() throws Exception {
        // first, locally write to /tmp/
        final Configuration hadoopConf = new Configuration();
        final Path path = Path.fromLocalFile(new File("/tmp/delta_core_flink_test_tables/table_000"));
        DeltaSource<RowData> source = DeltaSource.forBoundedRowData(path, hadoopConf).build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 1000));

        DataStream<RowData> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "delta-source");

        ClientAndIterator<RowData> client = DataStreamUtils.collectWithClient(stream, "Bounded Delta Source Test");

        int count = 0;
        while (client.iterator.hasNext()) {
            System.out.println(client.iterator.next());
            count++;
        }
        System.out.println("COUNT: " + count);
    }
}
