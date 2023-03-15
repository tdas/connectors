package io.delta.flink.table.it;

import java.util.ArrayList;
import java.util.List;

import io.delta.flink.utils.extensions.InMemoryCatalogExtension;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;

import static io.delta.flink.utils.DeltaTestUtils.buildCluster;

/*
To create the test table:

from pyspark.sql.functions import col

path = "/Users/scott.sandre/connectors/standalone/src/test/resources/delta/partitioned-table-small"

for i in range(15):
	low = i * 10
	high = low + 10
	spark.range(low, high).withColumn("part_a", col("id") % 3).withColumn("part_b", col("id") % 5).write.format("delta").partitionBy("part_a", "part_b").mode("append").save(path)

sql(f"select * from delta.`{path}`").show()

sql(f"select COUNT(*) from delta.`{path}`").show()
 */

public class SimpleDeltaCoreSQLSuite {

    @RegisterExtension
    private final InMemoryCatalogExtension catalogExtension = new InMemoryCatalogExtension();

    public void setupDeltaCatalog(TableEnvironment tableEnv) {
        catalogExtension.setupDeltaCatalog(tableEnv);
    }

    private static final int PARALLELISM = 2;

    private final MiniClusterWithClientResource miniClusterResource = buildCluster(PARALLELISM);

    public TableEnvironment tableEnv;

    // Why isn't this working?
    @BeforeEach
    public void setUp() {
        System.out.println("Scott > SimpleDeltaCoreSQLSuite > setUp");
        try {
            miniClusterResource.before();
            tableEnv = StreamTableEnvironment.create(getTestBatchEnv());
        } catch (Exception e) {
            throw new RuntimeException("Weren't able to setup the test dependencies", e);
        }
    }

    @AfterEach
    public void afterEach() {
        miniClusterResource.after();
    }

    @Test
    public void test_table_partition_push_down() throws Exception {
        if (tableEnv == null) {
            System.out.println("Scott > tableEnv was null");
            miniClusterResource.before();
            tableEnv = StreamTableEnvironment.create(getTestBatchEnv());
            setupDeltaCatalog(tableEnv);
        }

        String sourceTablePath = "../standalone/src/test/resources/delta/partitioned-table-small";

        String sourceTableSql = String.format(
            "CREATE TABLE sourceTable ("
                + " id BIGINT,"
                + " part_a BIGINT,"
                + " part_b BIGINT"
                + ") PARTITIONED BY (part_a, part_b) "
                + "WITH ("
                + " 'connector' = 'delta',"
                + " 'table-path' = '%s'"
                + ")",
            sourceTablePath);
        System.out.println("Scott > test_table_partition_push_down > sourceTableSql  " + sourceTableSql);
        tableEnv.executeSql(sourceTableSql);

        String selectSql = "SELECT * FROM sourceTable /*+ OPTIONS('mode' = 'batch') */ WHERE (part_a = 0 AND part_b = 0) OR (part_a = 1 AND part_b = 1)";
        System.out.println("Scott > test_table_partition_push_down > selectSql  " + selectSql);

        TableResult resultTable = tableEnv.executeSql(selectSql);

        try (CloseableIterator<Row> collect = resultTable.collect()) {
            while (collect.hasNext()) {
                Row row = collect.next();
                Object c0 = row.getField(0);
                Object c1 = row.getField(1);
                Object c2 = row.getField(2);
                System.out.println(String.format("%s, %s, %s", c0, c1, c2));
            }
        }
    }

    /** Copied from FlinkSqlTestITCase.java */
    private StreamExecutionEnvironment getTestBatchEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        return env;
    }
}
