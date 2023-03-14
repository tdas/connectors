/*
 * Copyright (2020-present) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.core.actions;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Represents an action that adds a new file to the table. The path of a file acts as the primary
 * key for the entry in the set of files.
 * <p>
 * Note: since actions within a given Delta file are not guaranteed to be applied in order, it is
 * not valid for multiple file operations with the same path to exist in a single version.
 *
 * @see  <a href="https://github.com/delta-io/delta/blob/master/PROTOCOL.md#add-file-and-remove-file">Delta Transaction Log Protocol: Add File and Remove File</a>
 */
public final class AddFile implements FileAction {
    private final String path;

    private final Map<String, String> partitionValues;

    private final long size;

    private final long modificationTime;

    private final boolean dataChange;

    private final String stats;

    private final Map<String, String> tags;

    public AddFile(
            String path,
            Map<String, String> partitionValues,
            long size,
            long modificationTime,
            boolean dataChange,
            String stats,
            Map<String, String> tags) {
        this.path = path;
        this.partitionValues = partitionValues;
        this.size = size;
        this.modificationTime = modificationTime;
        this.dataChange = dataChange;
        this.stats = stats;
        this.tags = tags;
    }

    /**
     * @return the corresponding {@link RemoveFile} for this file, instantiated with
     *         {@code deletionTimestamp =} {@link System#currentTimeMillis()}
     */
    
    public RemoveFile remove() {
        return remove(System.currentTimeMillis(), dataChange);
    }

    /**
     * @return the corresponding {@link RemoveFile} for this file, instantiated with the given
     *         {@code deletionTimestamp}
     */
    
    public RemoveFile remove(long deletionTimestamp) {
        return remove(deletionTimestamp, dataChange);
    }

    /**
     * @return the corresponding {@link RemoveFile} for this file, instantiated with the given
     *         {@code dataChange} flag
     */
    
    public RemoveFile remove(boolean dataChange) {
        return remove(System.currentTimeMillis(), dataChange);
    }

    /**
     * @return the corresponding {@link RemoveFile} for this file, instantiated with the given
     *         {@code deletionTimestamp} value and {@code dataChange} flag
     */
    
    public RemoveFile remove(long deletionTimestamp, boolean dataChange) {
        return new RemoveFile(path, Optional.of(deletionTimestamp), dataChange, true,
            partitionValues, Optional.of(size), tags);
    }

    /**
     * @return the relative path or the absolute path that should be added to the table. If it's a
     *         relative path, it's relative to the root of the table. Note: the path is encoded and
     *         should be decoded by {@code new java.net.URI(path)} when using it.
     */
    @Override
    
    public String getPath() {
        return path;
    }

    /**
     * @return an unmodifiable {@code Map} from partition column to value for
     *         this file. Partition values are stored as strings, using the following formats.
     *         An empty string for any type translates to a null partition value.
     * @see <a href="https://github.com/delta-io/delta/blob/master/PROTOCOL.md#Partition-Value-Serialization" target="_blank">Delta Protocol Partition Value Serialization</a>
     */
    
    public Map<String, String> getPartitionValues() {
        return Collections.unmodifiableMap(partitionValues);
    }

    /**
     * @return the size of this file in bytes
     */
    public long getSize() {
        return size;
    }

    /**
     * @return the time that this file was last modified or created, as
     *         milliseconds since the epoch
     */
    public long getModificationTime() {
        return modificationTime;
    }

    /**
     * @return whether any data was changed as a result of this file being created. When
     *         {@code false} the file must already be present in the table or the records in the
     *         added file must be contained in one or more remove actions in the same version
     */
    @Override
    public boolean isDataChange() {
        return dataChange;
    }

    /**
     * @return statistics (for example: count, min/max values for columns)
     *         about the data in this file as serialized JSON
     */
    
    public String getStats() {
        return stats;
    }

    /**
     * @return an unmodifiable {@code Map} containing metadata about this file
     */
    
    public Map<String, String> getTags() {
        return tags != null ? Collections.unmodifiableMap(tags) : null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AddFile addFile = (AddFile) o;
        return size == addFile.size &&
                modificationTime == addFile.modificationTime &&
                dataChange == addFile.dataChange &&
                Objects.equals(path, addFile.path) &&
                Objects.equals(partitionValues, addFile.partitionValues) &&
                Objects.equals(stats, addFile.stats) &&
                Objects.equals(tags, addFile.tags);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, partitionValues, size, modificationTime, dataChange, stats, tags);
    }

    /**
     * @return a new {@link Builder}
     */
    public static Builder builder(String path, Map<String, String> partitionValues, long size,
                                  long modificationTime, boolean dataChange) {
        return new Builder(path, partitionValues, size, modificationTime, dataChange);
    }

    /**
     * Builder class for {@link AddFile}. Enables construction of {@link AddFile}s with default
     * values.
     */
    public static final class Builder {
        // required AddFile fields
        private final String path;
        private final Map<String, String> partitionValues;
        private final long size;
        private final long modificationTime;
        private final boolean dataChange;

        // optional AddFile fields
        private String stats;
        private Map<String, String> tags;

        public Builder(String path, Map<String, String> partitionValues, long size,
                              long modificationTime, boolean dataChange) {
            this.path = path;
            this.partitionValues = partitionValues;
            this.size = size;
            this.modificationTime = modificationTime;
            this.dataChange = dataChange;
        }

        public Builder stats(String stats) {
            this.stats = stats;
            return this;
        }

        public Builder tags(Map<String, String> tags) {
            this.tags = tags;
            return this;
        }

        /**
         * Builds an {@link AddFile} using the provided parameters. If a parameter is not provided
         * its default values is used.
         *
         * @return a new {@link AddFile} with the properties added to the builder
         */
        public AddFile build() {
            AddFile addFile = new AddFile(this.path, this.partitionValues, this.size,
                    this.modificationTime, this.dataChange, this.stats, this.tags);
            return addFile;
        }
    }
}
