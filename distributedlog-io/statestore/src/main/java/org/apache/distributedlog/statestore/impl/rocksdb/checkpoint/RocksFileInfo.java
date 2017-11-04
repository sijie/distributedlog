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

import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * Represents an sst table file.
 */
@EqualsAndHashCode
@ToString
@Getter
@Slf4j
class RocksFileInfo {

    // sst file name
    private final String name;
    // ledger id to store the file
    private final long ledgerId;
    // data length
    private final long length;
    // links : who is linking to this file
    private final Set<String> links;

    RocksFileInfo(String name,
                  long ledgerId,
                  long length) {
        this.name = name;
        this.ledgerId = ledgerId;
        this.length = length;
        this.links = Collections.synchronizedSet(Sets.newHashSet());
    }

    int numLinks() {
        return links.size();
    }

    boolean link(String linkName) {
        return this.links.add(linkName);
    }

    boolean unlink(String linkName) {
        return this.links.remove(linkName);
    }

}
