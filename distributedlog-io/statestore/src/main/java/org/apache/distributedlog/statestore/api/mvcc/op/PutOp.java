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

package org.apache.distributedlog.statestore.api.mvcc.op;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import org.inferred.freebuilder.FreeBuilder;

@FreeBuilder
public interface PutOp<K, V> extends Op<K, V> {

    V value();

    boolean prevKV();

    class Builder<K, V> extends PutOp_Builder<K, V> {

        private Builder() {
            type(OpType.PUT);
            revision(-1L);
            prevKV(false);
        }

        @Override
        public PutOp<K, V> build() {
            checkArgument(type() == OpType.PUT, "Invalid type "  + type() + " is configured");
            checkNotNull(key(), "No key is specified");
            return super.build();
        }

    }

    static <K, V> Builder<K, V> newBuilder() {
        return new Builder<>();
    }

}
