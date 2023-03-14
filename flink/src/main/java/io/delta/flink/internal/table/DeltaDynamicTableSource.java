/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.delta.flink.internal.table;

import java.util.*;
import java.util.stream.Collectors;

import io.delta.flink.internal.table.DeltaFlinkJobSpecificOptions.TableMode;
import io.delta.flink.source.DeltaSource;
import io.delta.flink.source.internal.builder.DeltaSourceBuilderBase;
import io.delta.standalone.DeltaLog;
import io.delta.standalone.actions.Metadata;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsPartitionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;

/**
 * Implementation of {@link ScanTableSource} interface for Table/SQL support for Delta Source
 * connector.
 */
public class DeltaDynamicTableSource implements ScanTableSource, SupportsPartitionPushDown {

    private final Configuration hadoopConf;

    private final ReadableConfig tableOptions;

    private final List<String> columns;

    /** Partition Push Down **/
    private Optional<List<Map<String, String>>> remainingPartitions;

    /**
     * Constructor for creating Source of Flink dynamic table to Delta table.
     *
     * @param hadoopConf   Hadoop's configuration.
     * @param tableOptions Table options returned by Catalog and resolved query plan.
     * @param columns      Table's columns to extract from Delta table.
     */
    public DeltaDynamicTableSource(
            Configuration hadoopConf,
            ReadableConfig tableOptions,
            List<String> columns) {

        this.hadoopConf = hadoopConf;
        this.tableOptions = tableOptions;
        this.columns = columns;
        this.remainingPartitions = Optional.empty();
    }

    public DeltaDynamicTableSource(
            Configuration hadoopConf,
            ReadableConfig tableOptions,
            List<String> columns,
            Optional<List<Map<String, String>>> remainingPartition) {
        this.hadoopConf = hadoopConf;
        this.tableOptions = tableOptions;
        this.columns = columns;
        this.remainingPartitions = remainingPartition;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        TableMode mode = tableOptions.get(DeltaFlinkJobSpecificOptions.MODE);
        String tablePath = tableOptions.get(DeltaTableConnectorOptions.TABLE_PATH);
        System.out.println("Scott > DeltaDynamicTableSource > getScanRuntimeProvider > mode " + mode.toString());

        DeltaSourceBuilderBase<RowData, ?> sourceBuilder;

        switch (mode) {
            case BATCH:
                sourceBuilder = DeltaSource.forBoundedRowData(new Path(tablePath), hadoopConf);
                break;
            case STREAMING:
                sourceBuilder = DeltaSource.forContinuousRowData(new Path(tablePath), hadoopConf);
                break;
            default:
                throw new RuntimeException(
                    String.format(
                        "Unrecognized table mode %s used for Delta table %s",
                        mode, tablePath
                    ));
        }

        sourceBuilder.columnNames(columns);
        remainingPartitions.ifPresent(sourceBuilder::partitionPushDown);

        System.out.println("Scott > remainingPartitions > " + remainingPartitions);
        return SourceProvider.of(sourceBuilder.build());
    }

    @Override
    public DynamicTableSource copy() {
        System.out.println("Scott > DynamicTableSource > copy");
        return new DeltaDynamicTableSource(this.hadoopConf, this.tableOptions, this.columns, this.remainingPartitions);
    }

    @Override
    public String asSummaryString() {
        return "DeltaSource";
    }

    /////////////////////////
    // Partition Push Down //
    /////////////////////////

    /**
     * Returns a list of all partitions that a source can read if available.
     * A single partition maps each partition key to a partition value.
     * If Optional.empty() is returned, the list of partitions is queried from the catalog.
     */
    @Override
    public Optional<List<Map<String, String>>> listPartitions() {
        System.out.println("Scott > DeltaDynamicTableSource > listPartitions called!!!");
        final String tablePath = tableOptions.get(DeltaTableConnectorOptions.TABLE_PATH);
        final io.delta.standalone.DeltaLog log = DeltaLog.forTable(hadoopConf, tablePath);
        final Metadata latestMetadata = log.update().getMetadata();
        final List<Map<String, String>> output = new ArrayList<>();

        if (!latestMetadata.getPartitionColumns().isEmpty()) {
            log.snapshot().scan().getFiles().forEachRemaining(addFile -> {
                final String vals = addFile
                    .getPartitionValues()
                    .entrySet()
                    .stream()
                    .map(entry -> entry.getKey() + "->" + entry.getValue())
                    .collect(Collectors.joining(", "));
                System.out.println("Scott > DeltaDynamicTableSource > listPartitions :: " + vals);

                output.add(addFile.getPartitionValues());
            });
        }

        return Optional.of(output);
    }

    @Override
    public void applyPartitions(List<Map<String, String>> remainingPartitions) {
        System.out.println("Scott > DeltaDynamicTableSource > applyPartitions");

        if (remainingPartitions != null && !remainingPartitions.isEmpty()) {
            this.remainingPartitions = Optional.of(remainingPartitions);
        }
    }
}
