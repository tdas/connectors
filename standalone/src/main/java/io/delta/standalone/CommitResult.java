package io.delta.standalone;

/**
 * Wrapper around the result of {@link OptimisticTransaction#commit(Iterable, Operation, String)}..
 */
public class CommitResult {
    private final long version;

    public CommitResult(long version) {
        this.version = version;
    }

    /**
     * @return the table version that was committed.
     */
    public long getVersion() {
        return version;
    }
}