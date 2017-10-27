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

package org.apache.distributedlog.statestore.impl.mvcc;

import io.netty.buffer.ByteBuf;
import io.netty.util.Recycler;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.apache.distributedlog.common.coder.Coder;
import org.apache.distributedlog.common.util.Recycled;

/**
 * An object represents the mvcc metdata and value for a given key.
 */
@Data
@Setter
@Getter
public class MVCCRecord implements Recycled {

    public static MVCCRecord newRecord() {
        return RECYCLER.get();
    }

    private static final Recycler<MVCCRecord> RECYCLER = new Recycler<MVCCRecord>() {
        @Override
        protected MVCCRecord newObject(Handle<MVCCRecord> handle) {
            return new MVCCRecord(handle);
        }
    };

    private ByteBuf value;
    private final Recycler.Handle<MVCCRecord> handle;
    private long createRev;
    private long modRev;
    private long version;

    private MVCCRecord(Recycler.Handle<MVCCRecord> handle) {
        this.handle = handle;
    }

    public MVCCRecord duplicate() {
        MVCCRecord record = newRecord();
        record.createRev = createRev;
        record.modRev = modRev;
        record.version = version;
        record.value = value.retainedSlice();
        return record;
    }

    public int compareModRev(long revision) {
        return Long.compare(modRev, revision);
    }

    public int compareCreateRev(long revision) {
        return Long.compare(createRev, revision);
    }

    public int compareVersion(long version) {
        return Long.compare(this.version, version);
    }

    public void setValue(ByteBuf buf) {
        if (null != value) {
            value.release();
        }
        this.value = buf;
    }

    private void reset() {
        if (null != value) {
            value.release();
            value = null;
        }
        modRev = -1L;
        createRev = -1L;
        version = -1L;
    }

    @Override
    public void recycle() {
        reset();
        handle.recycle(this);
    }

    <K, V> KVRecordImpl<K, V> asKVRecord(KVRecordFactory<K, V> recordFactory,
                                         K key,
                                         Coder<V> valCoder) {
        KVRecordImpl<K, V> kv = recordFactory.newRecord();
        kv.setKey(key);
        kv.setValue(valCoder.decode(value));
        kv.setCreateRevision(createRev);
        kv.setModRevision(modRev);
        kv.setVersion(version);
        return kv;
    }
}
