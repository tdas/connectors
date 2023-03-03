package io.delta.standalone.core;

public interface RowIndexFilter {
    // for now
    void materializeIntoVector(long start, long end, boolean[] batch);
}