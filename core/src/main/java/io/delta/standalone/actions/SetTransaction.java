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

import java.util.Optional;

/**
 * Sets the committed version for a given application. Used to make operations like
 * Operation.Name#STREAMING_UPDATEs idempotent.
 *
 * @see  <a href="https://github.com/delta-io/delta/blob/master/PROTOCOL.md#transaction-identifiers">Delta Transaction Log Protocol: Transaction Identifiers</a>
 */
public final class SetTransaction implements Action {
    
    private final String appId;

    private final long version;

    
    private final Optional<Long> lastUpdated;

    public SetTransaction(
             String appId,
            long version,
             Optional<Long> lastUpdated) {
        this.appId = appId;
        this.version = version;
        this.lastUpdated = lastUpdated;
    }

    /**
     * @return the unique identifier for the application performing the transaction
     */
    
    public String getAppId() {
        return appId;
    }

    /**
     * @return the application-specific numeric identifier for this transaction
     */
    public long getVersion() {
        return version;
    }

    /**
     * @return the time when this transaction action was created, in milliseconds since the Unix
     *         epoch
     */
    
    public Optional<Long> getLastUpdated() {
        return lastUpdated;
    }
}
