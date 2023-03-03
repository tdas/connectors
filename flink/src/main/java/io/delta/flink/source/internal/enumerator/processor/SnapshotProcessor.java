package io.delta.flink.source.internal.enumerator.processor;

import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;

import io.delta.core.SimpleScanHelper;
import io.delta.flink.sink.internal.committer.DeltaCommitter;
import io.delta.flink.source.internal.enumerator.monitor.ChangesPerVersion;
import io.delta.flink.source.internal.file.AddFileEnumerator;
import io.delta.flink.source.internal.state.DeltaEnumeratorStateCheckpointBuilder;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import io.delta.flink.source.internal.utils.SourceUtils;
import io.delta.standalone.DeltaScan;
import io.delta.standalone.core.DeltaScanCore;
import io.delta.standalone.core.DeltaScanHelper;
import io.delta.standalone.core.DeltaScanTaskCore;
import io.delta.standalone.core.DeltaSnapshotCore;
import io.delta.standalone.utils.CloseableIterator;
import org.apache.flink.core.fs.Path;

import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.AddFile;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This implementation of {@link TableProcessor} process data from Delta table {@link Snapshot}.
 */
public class SnapshotProcessor extends TableProcessorBase {

    private static final Logger LOG = LoggerFactory.getLogger(SnapshotProcessor.class);

    /**
     * A {@link Snapshot} that is processed by this processor.
     */
    private final Snapshot snapshot;

    /**
     * Set with already processed paths for Parquet Files. Processor will skip (i.e. not process)
     * parquet files from this set.
     * <p>
     * The use case for this set is a recovery from checkpoint scenario, where we don't want to
     * reprocess already processed Parquet files.
     */
    private final HashSet<Path> alreadyProcessedPaths;

    private final DeltaSnapshotCore deltaSnapshotCore;

    private final DeltaScanHelper deltaScanHelper;

    public SnapshotProcessor(
            Path deltaTablePath,
            Snapshot snapshot,
            AddFileEnumerator<DeltaSourceSplit> fileEnumerator,
            Collection<Path> alreadyProcessedPaths) {
        super(deltaTablePath, fileEnumerator);
        this.snapshot = snapshot;
        this.alreadyProcessedPaths = new HashSet<>(alreadyProcessedPaths);
        this.deltaSnapshotCore = null;
        this.deltaScanHelper = null;
        throw new RuntimeException("Scott > SnapshotProcessor > not using deltaSnapshotCore");
    }

    public SnapshotProcessor(
            Path deltaTablePath,
            Snapshot snapshot,
            AddFileEnumerator<DeltaSourceSplit> fileEnumerator,
            Collection<Path> alreadyProcessedPaths,
            DeltaSnapshotCore deltaSnapshotCore,
            Configuration hadoopConf) {
        super(deltaTablePath, fileEnumerator);
        this.snapshot = snapshot;
        this.alreadyProcessedPaths = new HashSet<>(alreadyProcessedPaths);

        LOG.info("Scott > SnapshotProcessor > using deltaSnapshotCore");
        System.out.println("Scott > SnapshotProcessor > using deltaSnapshotCore");
        this.deltaScanHelper = new SimpleScanHelper();
        this.deltaSnapshotCore = deltaSnapshotCore;
    }

    /**
     * Process all {@link AddFile} from {@link Snapshot} passed to this {@code SnapshotProcessor}
     * constructor by converting them to {@link DeltaSourceSplit} objects.
     *
     * @param processCallback A {@link Consumer} callback that will be called after converting all
     *                        {@link AddFile} to {@link DeltaSourceSplit}.
     */
    @Override
    public void process(Consumer<List<DeltaSourceSplit>> processCallback) {
        System.out.println("Scott > SnapshotProcessor > process");

//        final DeltaScanCore deltaScanCore = deltaSnapshotCore.scan(deltaScanHelper);
        final DeltaScan deltaStandaloneScan = snapshot.scan(deltaScanHelper);
        System.out.println("Scott > SnapshotProcessor > process :: created deltaScanCore");
        final List<DeltaSourceSplit> splits = new ArrayList<>();

        try (CloseableIterator<DeltaScanTaskCore> iter = deltaStandaloneScan.getTasks()) {
            System.out.println("Scott > SnapshotProcessor > process :: created iter" + iter);
            System.out.println("Scott > SnapshotProcessor > process :: iter has next?" +
                iter.hasNext());

            while (iter.hasNext()) {
                final DeltaScanTaskCore task = iter.next();
                System.out.println("Scott > SnapshotProcessor > process :: created task");
                LOG.info("Scott > SnapshotProcessor > created task for path {}", task.getFilePath());
                System.out.println("Scott > SnapshotProcessor > created task for path " + task.getFilePath());
                final Path filePath = new Path(task.getFilePath());
                final DeltaSourceSplit split = new DeltaSourceSplit(
                    task.getPartitionValues(), // partitionValues
                    UUID.randomUUID().toString(), // id
                    filePath, // filePath
                    0L, // offset
                    0L, // length
                    task);
                splits.add(split);
                alreadyProcessedPaths.add(filePath);
            }
            System.out.println("Scott > SnapshotProcessor > process :: done with iter");
        } catch (IOException e) {
            throw new RuntimeException("Scott > SnapshotProcessor > process :: error", e);
        }


        // TODO Initial data read. This should be done in chunks since snapshot.getAllFiles()
        //  can have millions of files, and we would OOM the Job Manager
        //  if we would read all of them at once.
//        List<DeltaSourceSplit> splits =
//            prepareSplits(new ChangesPerVersion<>(
//                    SourceUtils.pathToString(deltaTablePath),
//                    snapshot.getVersion(),
//                    snapshot.getAllFiles()),
//                alreadyProcessedPaths::add);

        System.out.println("Scott > SnapshotProcessor > process :: done " + splits.size());
        processCallback.accept(splits);
    }

    @Override
    public DeltaEnumeratorStateCheckpointBuilder<DeltaSourceSplit> snapshotState(
        DeltaEnumeratorStateCheckpointBuilder<DeltaSourceSplit> checkpointBuilder) {

        checkpointBuilder.withProcessedPaths(alreadyProcessedPaths);

        // false means that this processor does not check Delta table for changes.
        checkpointBuilder.withMonitoringForChanges(false);
        return checkpointBuilder;
    }

    /**
     * @return A {@link Snapshot} version that this processor reads.
     */
    @Override
    public long getSnapshotVersion() {
        return snapshot.getVersion();
    }
}
