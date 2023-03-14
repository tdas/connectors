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

package io.delta.standalone.actions;

import java.util.*;

import io.delta.standalone.types.StructType;

/**
 * Updates the metadata of the table. The first version of a table must contain
 * a {@link Metadata} action. Subsequent {@link Metadata} actions completely
 * overwrite the current metadata of the table. It is the responsibility of the
 * writer to ensure that any data already present in the table is still valid
 * after any change. There can be at most one {@link Metadata} action in a
 * given version of the table.
 *
 * @see  <a href="https://github.com/delta-io/delta/blob/master/PROTOCOL.md#change-metadata">Delta Transaction Log Protocol: Change Metadata</a>
 */
public final class Metadata implements Action {
     private final String id;
     private final String name;
     private final String description;
     private final Format format;
     private final List<String> partitionColumns;
     private final Map<String, String> configuration;
     private final Optional<Long> createdTime;
     private final StructType schema;

    public Metadata(
             String id,
             String name,
             String description,
             Format format,
             List<String> partitionColumns,
             Map<String, String> configuration,
             Optional<Long> createdTime,
             StructType schema) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.format = format;
        this.partitionColumns = partitionColumns;
        this.configuration = configuration;
        this.createdTime = createdTime;
        this.schema = schema;
    }

    /**
     * @return the unique identifier for this table
     */
    
    public String getId() {
        return id;
    }

    /**
     * @return the user-provided identifier for this table
     */
    
    public String getName() {
        return name;
    }

    /**
     * @return the user-provided description for this table
     */
    
    public String getDescription() {
        return description;
    }

    /**
     * @return the {@link Format} for this table
     */
    
    public Format getFormat() {
        return format;
    }

    /**
     * @return an unmodifiable {@code java.util.List} containing the names of
     *         columns by which the data should be partitioned
     */
    
    public List<String> getPartitionColumns() {
        return Collections.unmodifiableList(partitionColumns);
    }

    /**
     * @return an unmodifiable {@code java.util.Map} containing configuration
     *         options for this metadata
     */
    
    public Map<String, String> getConfiguration() {
        return Collections.unmodifiableMap(configuration);
    }

    /**
     * @return the time when this metadata action was created, in milliseconds
     *         since the Unix epoch
     */
    
    public Optional<Long> getCreatedTime() {
        return createdTime;
    }

    /**
     * @return the schema of the table as a {@link StructType}
     */
    
    public StructType getSchema() {
        return schema;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Metadata metadata = (Metadata) o;
        return Objects.equals(id, metadata.id) &&
                Objects.equals(name, metadata.name) &&
                Objects.equals(description, metadata.description) &&
                Objects.equals(format, metadata.format) &&
                Objects.equals(partitionColumns, metadata.partitionColumns) &&
                Objects.equals(configuration, metadata.configuration) &&
                Objects.equals(createdTime, metadata.createdTime) &&
                Objects.equals(schema, metadata.schema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, description, format, partitionColumns, configuration,
                createdTime, schema);
    }

    /**
     * @return a new {@link Builder} initialized with the same properties as this
     *         {@link Metadata} instance
     */
    public Builder copyBuilder() {
        return new Builder(id, name, description, format, partitionColumns, configuration,
                createdTime, schema);
    }

    /**
     * @return a new {@link Builder}
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder class for {@link Metadata}. Enables construction of {@link Metadata}s with default
     * values.
     */
    public static final class Builder {
         private String id = java.util.UUID.randomUUID().toString();
         private String name;
         private String description;
         private Format format = new Format("parquet", Collections.emptyMap());
         private List<String> partitionColumns = Collections.emptyList();
         private Map<String, String> configuration = Collections.emptyMap();
         private Optional<Long> createdTime = Optional.of(System.currentTimeMillis());
         private StructType schema;

        public Builder(){};

        private Builder(
                 String id,
                 String name,
                 String description,
                 Format format,
                 List<String> partitionColumns,
                 Map<String, String> configuration,
                 Optional<Long> createdTime,
                 StructType schema) {
            this.id = id;
            this.name = name;
            this.description = description;
            this.format = format;
            this.partitionColumns = partitionColumns;
            this.configuration = configuration;
            this.createdTime = createdTime;
            this.schema = schema;
        }

        public Builder id( String id) {
            this.id = id;
            return this;
        }

        public Builder name( String name) {
            this.name = name;
            return this;
        }

        public Builder description( String description) {
            this.description = description;
            return this;
        }

        public Builder format( Format format) {
            this.format = format;
            return this;
        }

        public Builder partitionColumns( List<String> partitionColumns) {
            this.partitionColumns = partitionColumns;
            return this;
        }

        public Builder configuration( Map<String, String> configuration) {
            this.configuration = configuration;
            return this;
        }

        public Builder createdTime(Long createdTime) {
            this.createdTime = Optional.of(createdTime);
            return this;
        }

        public Builder createdTime( Optional<Long> createdTime) {
            this.createdTime = createdTime;
            return this;
        }

        public Builder schema( StructType schema) {
            this.schema = schema;
            return this;
        }

        /**
         * Builds a {@link Metadata} using the provided parameters. If a parameter is not provided
         * its default values is used.
         *
         * @return a new {@link Metadata} with the properties added to the builder
         */
        public Metadata build() {
            Metadata metadata = new Metadata(
                    this.id,
                    this.name,
                    this.description,
                    this.format,
                    this.partitionColumns,
                    this.configuration,
                    this.createdTime,
                    this.schema);
            return metadata;
        }
    }
}
