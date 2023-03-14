package io.delta.flink.source.internal.builder;

import java.util.*;

import io.delta.flink.internal.options.DeltaConnectorConfiguration;
import io.delta.flink.internal.options.DeltaOptionValidationException;
import io.delta.flink.internal.options.OptionValidator;
import io.delta.flink.source.DeltaSource;
import io.delta.flink.source.internal.DeltaSourceOptions;
import io.delta.flink.source.internal.enumerator.supplier.SnapshotSupplier;
import io.delta.flink.source.internal.enumerator.supplier.SnapshotSupplierFactory;
import io.delta.flink.source.internal.exceptions.DeltaSourceExceptions;
import io.delta.flink.source.internal.file.AddFileEnumerator;
import io.delta.flink.source.internal.file.DeltaFileEnumerator;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import io.delta.flink.source.internal.utils.SourceSchema;
import io.delta.flink.source.internal.utils.SourceUtils;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.connector.file.src.assigners.LocalityAwareSplitAssigner;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.StringUtils;
import org.apache.hadoop.conf.Configuration;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;

/**
 * The base class for {@link io.delta.flink.source.DeltaSource} builder.
 * <p>
 * This builder carries a <i>SELF</i> type to make it convenient to extend this for subclasses,
 * using the following pattern.
 *
 * <pre>{@code
 * public class SubBuilder<T> extends DeltaSourceBuilderBase<T, SubBuilder<T>> {
 *     ...
 * }
 * }</pre>
 *
 * <p>That way, all return values from builder method defined here are typed to the sub-class
 * type and support fluent chaining.
 *
 * <p>We don't make the publicly visible builder generic with a SELF type, because it leads to
 * generic signatures that can look complicated and confusing.
 *
 * @param <T> A type that this source produces.
 */
public abstract class DeltaSourceBuilderBase<T, SELF> {

    /**
     * The provider for {@link FileSplitAssigner}.
     */
    protected static final FileSplitAssigner.Provider DEFAULT_SPLIT_ASSIGNER =
        LocalityAwareSplitAssigner::new;

    /**
     * The provider for {@link AddFileEnumerator}.
     */
    protected static final AddFileEnumerator.Provider<DeltaSourceSplit>
        DEFAULT_SPLITTABLE_FILE_ENUMERATOR = DeltaFileEnumerator::new;

    /**
     * Default reference value for column names list.
     */
    protected static final List<String> DEFAULT_COLUMNS = new ArrayList<>(0);

    protected static final List<Map<String, String>> EMPTY_PUSHDOWN_PARTITIONS = Collections.emptyList();

    /**
     * Message prefix for validation exceptions.
     */
    protected static final String EXCEPTION_PREFIX = "DeltaSourceBuilder - ";

    /**
     * A placeholder object for Delta source configuration used for {@link DeltaSourceBuilderBase}
     * instance.
     */
    protected final DeltaConnectorConfiguration sourceConfiguration =
        new DeltaConnectorConfiguration();
    /**
     * Validates source configuration options.
     */
    private final OptionValidator optionValidator;

    /**
     * A {@link Path} to Delta table that should be read by created {@link
     * io.delta.flink.source.DeltaSource}.
     */
    protected final Path tablePath;

    /**
     * The Hadoop's {@link Configuration} for this Source.
     */
    protected final Configuration hadoopConfiguration;

    protected final SnapshotSupplierFactory snapshotSupplierFactory;

    /**
     * An array with Delta table's column names that should be read.
     */
    protected List<String> userColumnNames;

    /** Must be serializable, so can't be Optional. */
    protected List<Map<String, String>> pushdownPartitions;

    protected DeltaSourceBuilderBase(
            Path tablePath,
            Configuration hadoopConfiguration,
            SnapshotSupplierFactory snapshotSupplierFactory) {
        this.tablePath = tablePath;
        this.hadoopConfiguration = hadoopConfiguration;
        this.snapshotSupplierFactory = snapshotSupplierFactory;
        this.userColumnNames = DEFAULT_COLUMNS;
        this.optionValidator = new OptionValidator(tablePath,
                sourceConfiguration,
                DeltaSourceOptions.USER_FACING_SOURCE_OPTIONS);
        this.pushdownPartitions = EMPTY_PUSHDOWN_PARTITIONS;
    }

    public SELF partitionPushDown(List<Map<String, String>> partitions) {
        System.out.println("Scott > DeltaSourceBuilderBase > partitionPushDown");
        assert(partitions != null);
        assert(!partitions.isEmpty());
        this.pushdownPartitions = partitions;
        return self();
    }

    /**
     * Sets a {@link List} of column names that should be read from Delta table.
     */
    public SELF columnNames(List<String> columnNames) {
        this.userColumnNames = columnNames;
        return self();
    }

    /**
     * Sets a configuration option.
     */
    public SELF option(String optionName, String optionValue) {
        optionValidator.option(optionName, optionValue);
        return self();
    }

    /**
     * Sets a configuration option.
     */
    public SELF option(String optionName, boolean optionValue) {
        optionValidator.option(optionName, optionValue);
        return self();
    }

    /**
     * Sets a configuration option.
     */
    public SELF option(String optionName, int optionValue) {
        optionValidator.option(optionName, optionValue);
        return self();
    }

    /**
     * Sets a configuration option.
     */
    public SELF option(String optionName, long optionValue) {
        optionValidator.option(optionName, optionValue);
        return self();
    }

    /**
     * @return A copy of {@link DeltaConnectorConfiguration} used by builder. The changes made on
     * returned copy do not change the state of builder's configuration.
     */
    public DeltaConnectorConfiguration getSourceConfiguration() {
        return sourceConfiguration.copy();
    }

    public abstract <V extends DeltaSource<T>> V build();

    /**
     * This method should implement any logic for validation of mutually exclusive options.
     *
     * @return {@link Validator} instance with validation error message.
     */
    protected abstract Validator validateOptionExclusions();

    protected abstract Collection<String> getApplicableOptions();

    /**
     * Validate definition of Delta source builder including mandatory and optional options.
     */
    protected void validate() {
        Validator mandatoryValidator = validateMandatoryOptions();
        Validator exclusionsValidator = validateOptionExclusions();
        Validator inapplicableOptionValidator = validateInapplicableOptions();
        Validator optionalValidator = validateOptionalParameters();

        List<String> validationMessages = new LinkedList<>();

        validationMessages.addAll(mandatoryValidator.getValidationMessages());
        validationMessages.addAll(exclusionsValidator.getValidationMessages());
        validationMessages.addAll(optionalValidator.getValidationMessages());
        validationMessages.addAll(inapplicableOptionValidator.getValidationMessages());

        if (!validationMessages.isEmpty()) {
            String tablePathString =
                (tablePath != null) ? SourceUtils.pathToString(tablePath) : "null";
            throw new DeltaOptionValidationException(tablePathString, validationMessages);
        }
    }

    protected Validator validateMandatoryOptions() {

        return new Validator()
            // validate against null references
            .checkNotNull(tablePath, EXCEPTION_PREFIX + "missing path to Delta table.")
            .checkNotNull(hadoopConfiguration, EXCEPTION_PREFIX + "missing Hadoop configuration.");
    }

    protected Validator validateOptionalParameters() {
        Validator validator = new Validator();

        if (userColumnNames != DEFAULT_COLUMNS) {
            validator.checkNotNull(userColumnNames,
                EXCEPTION_PREFIX + "used a null reference for user columns.");

            if (userColumnNames != null) {
                validator.checkArgument(!userColumnNames.isEmpty(),
                    EXCEPTION_PREFIX + "user column names list is empty.");
                if (!userColumnNames.isEmpty()) {
                    validator.checkArgument(
                        userColumnNames.stream().noneMatch(StringUtils::isNullOrWhitespaceOnly),
                        EXCEPTION_PREFIX
                            + "user column names list contains at least one element that is null, "
                            + "empty, or has only whitespace characters.");
                }
            }
        }

        return validator;
    }

    /**
     * Validated builder options that were used but they might be not applicable for given builder
     * type, for example using options from bounded mode like "versionAsOf" for continuous mode
     * builder.
     *
     * @return The {@link Validator} object with all (if any) validation error messages.
     */
    protected Validator validateInapplicableOptions() {

        Validator validator = new Validator();
        sourceConfiguration.getUsedOptions()
            .stream()
            .filter(DeltaSourceOptions::isUserFacingOption)
            .forEach(usedOption ->
                validator.checkArgument(getApplicableOptions().contains(usedOption),
                prepareInapplicableOptionMessage(
                    sourceConfiguration.getUsedOptions(),
                    getApplicableOptions())
            ));

        return validator;
    }

    protected String prepareOptionExclusionMessage(String... mutualExclusiveOptions) {
        return String.format(
            "Used mutually exclusive options for Source definition. Invalid options [%s]",
            String.join(",", mutualExclusiveOptions));
    }

    protected String prepareInapplicableOptionMessage(
            Collection<String> usedOptions,
            Collection<String> applicableOptions) {
        return String.format(
            "Used inapplicable option for source configuration. Used options [%s], applicable "
                + "options [%s]",
            usedOptions, applicableOptions);
    }

    /**
     * Extracts Delta table schema from DeltaLog {@link io.delta.standalone.actions.Metadata}
     * including column names and column types converted to
     * {@link org.apache.flink.table.types.logical.LogicalType}.
     * <p>
     * If {@link #userColumnNames} were defined, only those columns will be included in extracted
     * schema.
     *
     * @return A {@link SourceSchema} including Delta table column names with their types that
     * should be read from Delta table.
     */
    protected SourceSchema getSourceSchema() {
        DeltaLog deltaLog =
            DeltaLog.forTable(hadoopConfiguration, SourceUtils.pathToString(tablePath));
        SnapshotSupplier snapshotSupplier = snapshotSupplierFactory.create(deltaLog);
        Snapshot snapshot = snapshotSupplier.getSnapshot(sourceConfiguration);

        try {
            return SourceSchema.fromSnapshot(userColumnNames, snapshot);
        } catch (IllegalArgumentException e) {
            throw DeltaSourceExceptions.generalSourceException(
                SourceUtils.pathToString(tablePath),
                snapshot.getVersion(),
                e
            );
        }
    }

    @SuppressWarnings("unchecked")
    protected SELF self() {
        return (SELF) this;
    }
}
