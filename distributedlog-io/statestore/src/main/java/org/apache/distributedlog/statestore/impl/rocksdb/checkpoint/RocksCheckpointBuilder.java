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

package org.apache.distributedlog.statestore.impl.rocksdb.checkpoint;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.Map;

class RocksCheckpointBuilder {

    private final Map<String, RocksFileInfo> files;
    private RocksCheckpointState state;

    RocksCheckpointBuilder() {
        this.files = Maps.newHashMap();
    }

    synchronized RocksCheckpointBuilder add(String name, RocksFileInfo fi) {
        files.put(name, fi);
        return this;
    }

    synchronized RocksCheckpointBuilder setState(RocksCheckpointState state) {
        this.state = state;
        return this;
    }

    synchronized RocksCheckpoint complete() {
        checkArgument(RocksCheckpointState.COMMIT == state);
        return new RocksCheckpoint(
            state,
            ImmutableMap.copyOf(files));
    }

}
