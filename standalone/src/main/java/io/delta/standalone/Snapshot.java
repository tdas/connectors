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

package io.delta.standalone;

import java.util.List;

import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.core.DeltaScanHelper;
import io.delta.standalone.data.CloseableIterator;
import io.delta.standalone.data.RowRecord;
import io.delta.standalone.expressions.Expression;

/**
 * {@link Snapshot} provides APIs to access the Delta table state (such as table metadata, active
 * files) at some version.
 * <p>
 * See <a href="https://github.com/delta-io/delta/blob/master/PROTOCOL.md">Delta Transaction Log Protocol</a>
 * for more details about the transaction logs.
 */
public interface Snapshot {

    /**
     * @return a {@link DeltaScan} of the files in this snapshot
     */
    DeltaScan scan();

    /**
     * @return a {@link DeltaScan} of the files in this snapshot
     */
    DeltaScan scan(DeltaScanHelper scanHelper);

    /**
     * @param predicate  the predicate to be used to filter the files in this snapshot.
     * @return a {@link DeltaScan} of the files in this snapshot matching the pushed portion of
     *         {@code predicate}
     */
    DeltaScan scan(Expression predicate);

    /**
     * @return all of the files present in this snapshot
     */
    List<AddFile> getAllFiles();

    /**
     * @return the table metadata for this snapshot
     */
    Metadata getMetadata();

    /**
     * @return the version for this snapshot
     */
    long getVersion();

    /**
     * Creates a {@link CloseableIterator} which can iterate over data belonging to this snapshot.
     * It provides no iteration ordering guarantee among data.
     *
     * @return a {@link CloseableIterator} to iterate over data
     */
    CloseableIterator<RowRecord> open();
}
