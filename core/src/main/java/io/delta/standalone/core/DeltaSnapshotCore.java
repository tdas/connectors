package io.delta.standalone.core;

public interface DeltaSnapshotCore {
    DeltaScanCore scan(DeltaScanHelper scanHelper);
}
